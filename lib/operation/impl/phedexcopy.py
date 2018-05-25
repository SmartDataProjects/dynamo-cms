import logging
import collections

from dynamo.operation.copy import CopyInterface
from dynamo.utils.interface.webservice import POST
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.dataformat import DatasetReplica, Configuration

LOG = logging.getLogger(__name__)

class PhEDExCopyInterface(CopyInterface):
    """Copy using PhEDEx."""

    def __init__(self, config = None):
        config = Configuration(config)

        CopyInterface.__init__(self, config)

        self._phedex = PhEDEx(config.get('phedex', None))

        self.subscription_chunk_size = config.get('chunk_size', 50.) * 1.e+12

    def schedule_copy(self, replica,  comments = ''): #override
        request_mapping = {}

        subscription_list = []

        if type(replica) is DatasetReplica:
            blocks_by_group = collections.defaultdict(set)
            for block_replica in replica.block_replicas:
                blocks_by_group[block_replica.group].add(block_replica.block)

            if len(blocks_by_group) > 1:
                # this was called as a dataset-level copy, but in fact we have multiple
                # sets of blocks with different groups -> recall block-level schedule_copies
                return self.schedule_copies(replica.block_replicas, comments)

            group, block_replicas = blocks_by_group.items()[0]

            if block_replicas == replica.dataset.blocks:
                subscription_list.append(replica.dataset)
                level = 'dataset'
            else:
                subscription_list.extend(block_replicas)
                level = 'block'

        else: #BlockReplica
            group = replica.group
            subscription_list.append(replica.block)
            level = 'block'

        self._run_subscription_request(request_mapping, replica.site, group, level, subscription_list, comments)

        return request_mapping

    def schedule_copies(self, replicas, comments = ''): #override
        request_mapping = {}

        replicas_by_site = collections.defaultdict(list)
        for replica in replicas:
            replicas_by_site[replica.site].append(replica)

        for site, replica_list in replicas_by_site.iteritems():
            # sort the subscriptions into dataset level / block level and by groups
            subscription_lists = {}
            subscription_lists['dataset'] = collections.defaultdict(list) # {(level, group_name): [replicas]}
            subscription_lists['block'] = collections.defaultdict(list) # {(level, group_name): [replicas]}

            for replica in replica_list:
                if type(replica) is DatasetReplica:
                    blocks_by_group = collections.defaultdict(set)
                    for block_replica in replica.block_replicas:
                        blocks_by_group[block_replica.group].add(block_replica.block)

                    for group, blocks in blocks_by_group.iteritems():
                        if blocks == replica.dataset.blocks:
                            subscription_lists['dataset'][group].append(replica.dataset)
                        else:
                            subscription_lists['block'][group].extend(blocks)
                else:
                    subscription_lists['block'][replica.group].append(replica.block)

            for level in ['dataset', 'block']:
                for group, items in subscription_lists[level].iteritems():
                    self._run_subscription_request(request_mapping, site, group, level, items, comments)

        return request_mapping

    def _run_subscription_request(self, request_mapping, site, group, level, subscription_list, comments):
        # Make a subscription request for potentitally multiple datasets or blocks but to one site and one group
        full_catalog = collections.defaultdict(list)

        if level == 'dataset':
            for dataset in subscription_list:
                full_catalog[dataset] = []
        elif level == 'block':
            for block in subscription_list:
                full_catalog[block.dataset].append(block)

        LOG.info('Subscribing %d datasets for %s at %s', len(full_catalog), group.name, site.name)

        # make requests in chunks
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
            
            if chunk_size < self.subscription_chunk_size and len(full_catalog) != 0:
                continue

            options = {
                'node': site.name,
                'data': self._phedex.form_catalog_xml(request_catalog),
                'level': level,
                'priority': 'low',
                'move': 'n',
                'static': 'n',
                'custodial': 'n',
                'group': group.name,
                'request_only': 'n',
                'no_mail': 'n',
                'comments': comments
            }
    
            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response)
            if self.dry_run:
                result = [{'id': '%d' % self._next_operation_id}]
                self._next_operation_id += 1
            else:
                try:
                    result = self._phedex.make_request('subscribe', options, method = POST)
                except:
                    result = []

            if len(result) != 0:
                request_id = int(result[0]['id']) # return value is a string
                LOG.warning('PhEDEx subscription request id: %d', request_id)
                request_mapping[request_id] = (True, site, items)
            else:
                LOG.error('Copy %s failed.', str(options))
                # we should probably do something here

            request_catalog = {}
            chunk_size = 0
            items = []

    def copy_status(self, request_id): #override
        status = {}

        request = self._phedex.make_request('transferrequests', 'request=%d' % request_id)
        if len(request) == 0:
            return status

        # A single request can have multiple destinations
        site_names = [d['name'] for d in request[0]['destinations']['node']]

        dataset_names = []
        for ds_entry in request[0]['data']['dbs']['dataset']:
            dataset_names.append(ds_entry['name'])

        block_names = []
        for ds_entry in request[0]['data']['dbs']['block']:
            block_names.append(ds_entry['name'])

        if len(dataset_names) != 0:
            # Process dataset-level subscriptions

            subscriptions = []
            chunks = [dataset_names[i:i + 35] for i in xrange(0, len(dataset_names), 35)]
            for site_name in site_names:
                for chunk in chunks:
                    subscriptions.extend(self._phedex.make_request('subscriptions', ['node=%s' % site_name] + ['dataset=%s' % n for n in chunk]))

            for dataset in subscriptions:
                dataset_name = dataset['name']
                try:
                    cont = dataset['subscription'][0]
                except KeyError:
                    LOG.error('Subscription of %s should exist but doesn\'t', dataset_name)
                    continue

                site_name = cont['node']
                bytes = dataset['bytes']

                node_bytes = cont['node_bytes']
                if node_bytes is None:
                    node_bytes = 0
                elif node_bytes != bytes:
                    # it's possible that there were block-level deletions
                    blocks = self._phedex.make_request('blockreplicas', ['node=%s' % site_name, 'dataset=%s' % dataset_name])
                    bytes = sum(b['bytes'] for b in blocks)

                status[(site_name, dataset_name)] = (bytes, node_bytes, cont['time_update'])

        if len(block_names) != 0:
            # Process block-level subscriptions

            subscriptions = []
            chunks = [block_names[i:i + 35] for i in xrange(0, len(block_names), 35)]
            for site_name in site_names:
                for chunk in chunks:
                    subscriptions.extend(self._phedex.make_request('subscriptions', ['node=%s' % site_name] + ['block=%s' % n for n in chunk]))

            overridden = set()

            for dataset in subscriptions:
                dataset_name = dataset['name']

                try:
                    blocks = dataset['block']
                except KeyError:
                    try:
                        cont = dataset['subscription'][0]
                    except KeyError:
                        LOG.error('Subscription of %s neither block-level nor dataset-level', dataset_name)
                        continue

                    site_name = cont['node']

                    if (site_name, dataset_name) in overridden:
                        # this is a dataset-level subscription and we've processed this dataset already
                        continue

                    overridden.add((site_name, dataset_name))

                    LOG.debug('Block-level subscription of %s at %s is overridden', dataset_name, site_name)

                    requested_blocks = [name for name in block_names if name.startswith(dataset_name + '#')]

                    blocks = self._phedex.make_request('blockreplicas', ['node=%s' % site_name, 'dataset=%s' % dataset_name])
                    for block in blocks:
                        block_name = block['name']
                        if block_name not in requested_blocks:
                            continue
                        
                        replica = block['replica'][0]
                        status[(site_name, block_name)] = (block['bytes'], replica['bytes'], replica['time_update'])

                    continue

                for block in blocks:
                    block_name = block['name']
                    try:
                        cont = block['subscription'][0]
                    except KeyError:
                        LOG.error('Subscription of %s should exist but doesn\'t', block_name)
                        continue

                    node_bytes = cont['node_bytes']
                    if node_bytes is None:
                        node_bytes = 0

                    status[(cont['node'], block_name)] = (block['bytes'], node_bytes, cont['time_update'])

        # now we pick up whatever did not appear in the subscriptions call
        for site_name in site_names:
            for dataset_name in dataset_names:
                key = (site_name, dataset_name)
                if key not in status:
                    status[key] = None

            for block_name in block_names:
                key = (site_name, block_name)
                if key not in status:
                    status[key] = None

        return status
