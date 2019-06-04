import time
import logging
import collections

from dynamo.operation.deletion import DeletionInterface
from dynamo.utils.interface.webservice import POST
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.history.history import HistoryDatabase
from dynamo.dataformat import DatasetReplica, BlockReplica, Site, Group, Configuration

LOG = logging.getLogger(__name__)

class PhEDExDeletionInterface(DeletionInterface):
    """Deletion using PhEDEx."""

    def __init__(self, config = None):
        config = Configuration(config)

        DeletionInterface.__init__(self, config)

        self._phedex = PhEDEx(config.get('phedex', None))

        self._history = HistoryDatabase(config.get('history', None))

        self.auto_approval = config.get('auto_approval', True)
        self.allow_tape_deletion = config.get('allow_tape_deletion', True)
        self.tape_auto_approval = config.get('tape_auto_approval', False)

        self.deletion_chunk_size = config.get('chunk_size', 50.) * 1.e+12

    def schedule_deletions(self, replica_list, operation_id, comments = ''): #override
        sites = set(r.site for r, b in replica_list)
        if len(sites) != 1:
            raise OperationalError('schedule_copies should be called with a list of replicas at a single site.')

        site = list(sites)[0]

        if site.storage_type == Site.TYPE_MSS and not self.allow_tape_deletion:
            LOG.warning('Deletion from MSS not allowed by configuration.')
            return []

        if self.allow_tape_deletion and self.auto_approval:
            LOG.warning('You cannot have auto-approved tape deletions. Set auto-approval to False.')
            return []

        # execute the deletions in two steps: one for dataset-level and one for block-level
        datasets = []
        blocks = []

        # maps used later for cloning
        # getting ugly here.. should come up with a better way of making clones
        replica_map = {}
        block_replica_map = {}

        for dataset_replica, block_replicas in replica_list:
            if block_replicas is None:
                datasets.append(dataset_replica.dataset)
            else:
                blocks.extend(br.block for br in block_replicas)

                replica_map[dataset_replica.dataset] = dataset_replica
                block_replica_map.update((br.block, br) for br in block_replicas)

        success = []

        deleted_datasets = self._run_deletion_request(operation_id, site, 'dataset', datasets, comments)

        for dataset in deleted_datasets:
            replica = DatasetReplica(dataset, site, growing = False, group = Group.null_group)
            success.append((replica, None))

        tmp_map = dict((dataset, []) for dataset in replica_map.iterkeys())

        deleted_blocks = self._run_deletion_request(operation_id, site, 'block', blocks, comments)

        for block in deleted_blocks:
            tmp_map[block.dataset].append(block)

        for dataset, blocks in tmp_map.iteritems():
            replica = DatasetReplica(dataset, site)
            replica.copy(replica_map[dataset])
            
            success.append((replica, []))
            for block in blocks:
                block_replica = BlockReplica(block, site, Group.null_group)
                block_replica.copy(block_replica_map[block])
                block_replica.last_update = int(time.time())
                success[-1][1].append(block_replica)

        return success

    def _run_deletion_request(self, operation_id, site, level, deletion_list, comments):
        full_catalog = collections.defaultdict(list)

        if level == 'dataset':
            for dataset in deletion_list:
                full_catalog[dataset] = []
        elif level == 'block':
            for block in deletion_list:
                full_catalog[block.dataset].append(block)

        history_sql = 'INSERT INTO `phedex_requests` (`id`, `operation_type`, `operation_id`, `approved`) VALUES (%s, \'deletion\', %s, %s)'

        deleted_items = []

        request_catalog = {}
        chunk_size = 0
        items = []
        while len(full_catalog) != 0:
            dataset, blocks = full_catalog.popitem()
            request_catalog[dataset] = blocks

            if level == 'dataset':
                chunk_size += dataset.size
                items.append(dataset)
            elif level == 'block':
                chunk_size += sum(b.size for b in blocks)
                items.extend(blocks)

            if chunk_size < self.deletion_chunk_size and len(full_catalog) != 0:
                continue

            options = {
                'node': site.name,
                'data': self._phedex.form_catalog_xml(request_catalog),
                'level': level,
                'rm_subscriptions': 'y',
                'comments': comments
            }
    
            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response) if successful
            try:
                if self._read_only:
                    result = [{'id': 0}]
                else:
                    result = self._phedex.make_request('delete', options, method = POST)
            except:
                LOG.error('Deletion %s failed.', str(options))

                if self._phedex.last_errorcode == 400:
                    # Sometimes we have invalid data in the list of objects to delete.
                    # PhEDEx throws a 400 error in such a case. We have to then try to identify the
                    # problematic item through trial and error.
                    if len(items) == 1:
                        LOG.error('Could not delete %s from %s', str(items[0]), site.name)
                    else:
                        LOG.info('Retrying with a reduced item list.')
                        deleted_items.extend(self._run_deletion_request(operation_id, site, level, items[:len(items) / 2], comments))
                        deleted_items.extend(self._run_deletion_request(operation_id, site, level, items[len(items) / 2:], comments))
                else:
                    raise
            else:
                request_id = int(result[0]['id']) # return value is a string
                LOG.warning('PhEDEx deletion request id: %d', request_id)

                approved = False

                if self._read_only:
                    approved = True

                elif self.auto_approval:
                    try:
                        result = self._phedex.make_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': site.name}, method = POST)
                    except:
                        LOG.error('deletion approval of request %d failed.', request_id)
                    else:
                        approved = True

                if not self._read_only:
                    self._history.db.query(history_sql, request_id, operation_id, approved)

                if approved:
                    deleted_items.extend(items)

            request_catalog = {}
            chunk_size = 0
            items = []

        return deleted_items

    def deletion_status(self, request_id): #override
        request = self._phedex.make_request('deleterequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        node_info = request[0]['nodes']['node'][0]
        site_name = node_info['name']
        last_update = node_info['decided_by']['time_decided']

        status = {}
        for ds_entry in request[0]['data']['dbs']['dataset']:
            status[ds_entry['name']] = (ds_entry['bytes'], ds_entry['bytes'], last_update)
            
        return status
