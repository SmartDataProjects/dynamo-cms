import logging
import collections

from dynamo.source.replicainfo import ReplicaInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.utils.parallel import Map
from dynamo.dataformat import Group, Site, Dataset, Block, File
from dynamo.dataformat import DatasetReplica, BlockReplica, Configuration
from dynamo.dataformat import ObjectError

LOG = logging.getLogger(__name__)

class PhEDExReplicaInfoSource(ReplicaInfoSource):
    """ReplicaInfoSource using PhEDEx."""

    def __init__(self, config = None):
        if config is None:
            config = Configuration()

        ReplicaInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.get('phedex', None))
	self._parallelizer_config = config

    def replica_exists_at_site(self, site, item): #override
        options = ['node=' + site.name]
        if type(item) == Dataset:
            options += ['dataset=' + item.name, 'show_dataset=y']
        elif type(item) == DatasetReplica:
            options += ['dataset=' + item.dataset.name, 'show_dataset=y']
        elif type(item) == Block:
            options += ['block=' + item.full_name()]
        elif type(item) == BlockReplica:
            options += ['block=' + item.block.full_name()]
        else:
            raise RuntimeError('Invalid input passed: ' + repr(item))
        
        source = self._phedex.make_request('blockreplicas', options, timeout = 600)

        if len(source) != 0:
            return True

        options = ['node=' + site.name]
        if type(item) == Dataset:
            # check both dataset-level and block-level subscriptions
            options += ['dataset=' + item.name, 'block=%s#*' % item.name]
        elif type(item) == DatasetReplica:
            options += ['dataset=' + item.dataset.name, 'block=%s#*' % item.dataset.name]
        elif type(item) == Block:
            options += ['block=' + item.full_name()]
        elif type(item) == BlockReplica:
            options += ['block=' + item.block.full_name()]

        # blockreplicas has max ~20 minutes latency
        source = self._phedex.make_request('subscriptions', options, timeout = 600)

        return len(source) != 0

    def get_replicas(self, site = None, dataset = None, block = None): #override
        if site is None:
            site_check = self.check_allowed_site
        else:
            site_check = None
            if not self.check_allowed_site(site):
                return []

        if dataset is None and block is None:
            dataset_check = self.check_allowed_dataset
        else:
            dataset_check = None
            if dataset is not None:
                if not self.check_allowed_dataset(dataset):
                    return []
            if block is not None:
                if not self.check_allowed_dataset(block[:block.find('#')]):
                    return []

        options = []
        if site is not None:
            options.append('node=' + site)
        if dataset is not None:
            options.append('dataset=' + dataset)
        if block is not None:
            options.append('block=' + block)

        LOG.info('get_replicas(' + ','.join(options) + ')  Fetching the list of replicas from PhEDEx')

        if len(options) == 0:
            return []
        
        block_entries = self._phedex.make_request('blockreplicas', options, timeout = 7200)

        parallelizer = Map()
        parallelizer.timeout = 7200

        # Automatically starts a thread as we add the output of block_entries
        combine_file = parallelizer.get_starter(self._combine_file_info)

        for block_entry in block_entries:
            for replica_entry in block_entry['replica']:
                if replica_entry['complete'] == 'n':
                    break
            else:
                continue

            # there is at least one incomplete replica
            try:
                dataset_name, block_name = Block.from_full_name(block_entry['name'])
            except ObjectError: # invalid name
                continue

            if dataset_check and not dataset_check(dataset_name):
                continue

            combine_file.add_input(block_entry)

        combine_file.close()

        # _combine_file_info alters block_entries directly - no need to deal with output
        combine_file.get_outputs()

        block_replicas = PhEDExReplicaInfoSource.make_block_replicas(block_entries, PhEDExReplicaInfoSource.maker_blockreplicas, site_check = site_check, dataset_check = dataset_check)
        
        # Also use subscriptions call which has a lower latency than blockreplicas
        # For example, group change on a block replica at time T may not show up in blockreplicas until up to T + 15 minutes
        # while in subscriptions it is visible within a few seconds
        # But subscriptions call without a dataset or block takes too long
        if dataset is None and block is None:
            return block_replicas

        indexed = collections.defaultdict(dict)
        for replica in block_replicas:
            indexed[(replica.site.name, replica.block.dataset.name)][replica.block.name] = replica

        dataset_entries = self._phedex.make_request('subscriptions', options, timeout = 3600)

        for dataset_entry in dataset_entries:
            dataset_name = dataset_entry['name']

            if not self.check_allowed_dataset(dataset_name):
                continue

            try:
                subscriptions = dataset_entry['subscription']
            except KeyError:
                pass
            else:
                for sub_entry in subscriptions:
                    site_name = sub_entry['node']

                    if not self.check_allowed_site(site_name):
                        continue

                    replicas = indexed[(site_name, dataset_name)]

                    for replica in replicas.itervalues():
                        replica.group = Group(sub_entry['group'])
                        replica.is_custodial = (sub_entry['custodial'] == 'y')

            try:
                block_entries = dataset_entry['block']
            except KeyError:
                pass
            else:
                for block_entry in block_entries:
                    try:
                        _, block_name = Block.from_full_name(block_entry['name'])
                    except ObjectError:
                        continue

                    try:
                        subscriptions = block_entry['subscription']
                    except KeyError:
                        continue

                    for sub_entry in subscriptions:
                        site_name = sub_entry['node']

                        if not self.check_allowed_site(site_name):
                            continue

                        try:
                            replica = indexed[(site_name, dataset_name)][block_name]
                        except KeyError:
                            continue

                        replica.group = Group(sub_entry['group'])

                        if sub_entry['node_bytes'] == block_entry['bytes']:
                            # complete
                            replica.size = sub_entry['node_bytes']
                            if replica.size is None:
                                replica.size = 0
                            replica.files = None
                        else:
                            # incomplete - since we cannot know what files are there, we'll just have to pretend there is none
                            replica.size = 0
                            replica.files = tuple()

                        replica.is_custodial = (sub_entry['custodial'] == 'y')

                        if sub_entry['time_update'] is not None:
                            replica.last_update = 0
                        else:
                            replica.last_update = int(sub_entry['time_update'])

        return block_replicas

    def get_updated_replicas(self, updated_since, inventory): #override
        LOG.info('get_updated_replicas(%d)  Fetching the list of replicas from PhEDEx', updated_since)

        nodes = []
        for entry in self._phedex.make_request('nodes', timeout = 600):
            if not self.check_allowed_site(entry['name']):
                continue

            if entry['name'] not in inventory.sites:
                continue

            nodes.append(entry['name'])

        try:
            tmpconfig = Configuration(self._parallelizer_config.get('parallel', None))
        except Exception as e:
	    LOG.error(str(e))
            tmpconfig = Configuration()

	parallelizer = Map(tmpconfig)
        parallelizer.timeout = 5400

        def get_node_replicas(node):
            options = ['update_since=%d' % updated_since, 'node=%s' % node]
            results = self._phedex.make_request('blockreplicas', options)

            return node, results

        # Use async to fire threads on demand
        node_results = parallelizer.execute(get_node_replicas, nodes, async = True)

        # Automatically starts a thread as we add the output of block_replicas
        combine_file = parallelizer.get_starter(self._combine_file_info)

        all_block_entries = []

        for node, block_entries in node_results:
            site = inventory.sites[node]

            for block_entry in block_entries:
                all_block_entries.append(block_entry)

                replica_entry = block_entry['replica'][0]

                if replica_entry['complete'] == 'y':
                    continue

                # incomplete block replica - should we fetch file info?
                try:
                    dataset_name, block_name = Block.from_full_name(block_entry['name'])
                except ObjectError:
                    pass
                else:
                    try:
                        dataset = inventory.datasets[dataset_name]
                        block = dataset.find_block(block_name)
                        replica = block.find_replica(site)
                        if replica.file_ids is None:
                            num_files = block.num_files
                        else:
                            num_files = len(replica.file_ids)

                        if replica.size == replica_entry['bytes'] and num_files == replica_entry['files']:
                            # no we don't have to
                            continue
                    except:
                        # At any point of the above lookups we may hit a None object or KeyError or what not
                        pass
                        
                LOG.debug('Replica %s:%s is incomplete. Fetching file information.', replica_entry['node'], block_entry['name'])
                combine_file.add_input(block_entry)

        combine_file.close()

        # _combine_file_info alters block_entries directly - no need to deal with output
        combine_file.get_outputs()

        LOG.info('get_updated_replicas(%d) Got outputs' % updated_since)

        return PhEDExReplicaInfoSource.make_block_replicas(all_block_entries, PhEDExReplicaInfoSource.maker_blockreplicas, dataset_check = self.check_allowed_dataset)

    def get_deleted_replicas(self, deleted_since): #override
        LOG.info('get_deleted_replicas(%d)  Fetching the list of replicas from PhEDEx', deleted_since)

        result = self._phedex.make_request('deletions', ['complete_since=%d' % deleted_since], timeout = 7200)
        # result is by dataset
        block_entries = []
        for dataset_entry in result:
            block_entries.extend(dataset_entry['block'])

        return PhEDExReplicaInfoSource.make_block_replicas(block_entries, PhEDExReplicaInfoSource.maker_deletions)

    def _combine_file_info(self, block_entry):
        try:
            LOG.debug('_combine_file_info(%s) Fetching file replicas from PhEDEx', block_entry['name'])
            file_info = self._phedex.make_request('filereplicas', ['block=%s' % block_entry['name']])[0]['file']
        except (IndexError, KeyError):
            # Somehow PhEDEx didn't have a filereplicas entry for this block at this node
            block_entry['file'] = []
        else:
            block_entry['file'] = file_info

    @staticmethod
    def make_block_replicas(block_entries, replica_maker, site_check = None, dataset_check = None):
        """Return a list of block replicas linked to Dataset, Block, Site, and Group"""

        dataset = None
        block_replicas = []

        for block_entry in block_entries:
            try:
                dataset_name, block_name = Block.from_full_name(block_entry['name'])
            except ObjectError: # invalid name
                continue

            if dataset is None or dataset.name != dataset_name:
                if dataset_check and not dataset_check(dataset_name):
                    continue
    
                try:
                    dataset = Dataset(
                        dataset_name
                    )
                except ObjectError:
                    # invalid name
                    dataset = None

            if dataset is None:
                continue
            
            block = Block(
                block_name,
                dataset,
                block_entry['bytes']
            )
            if block.size is None:
                block.size = 0

            block_replicas.extend(replica_maker(block, block_entry, site_check = site_check))

        return block_replicas

    @staticmethod
    def maker_blockreplicas(block, block_entry, site_check = None):
        """Return a list of block replicas using blockreplicas data or a combination of blockreplicas and filereplicas calls."""

        sites = {}
        invalid_sites = set()
        groups = {}

        block_replicas = {}

        for replica_entry in block_entry['replica']:
            site_name = replica_entry['node']
            try:
                site = sites[site_name]
            except KeyError:
                if site_check:
                    if site_name in invalid_sites:
                        continue
                    if not site_check(site_name):
                        invalid_sites.add(site_name)
                        continue
    
                site = sites[site_name] = Site(site_name)

            group_name = replica_entry['group']
            try:
                group = groups[group_name]
            except KeyError:
                group = groups[group_name] = Group(group_name)

            try:
                time_update = int(replica_entry['time_update'])
            except TypeError:
                # time_update was None
                time_update = 0

            block_replica = BlockReplica(
                block,
                site,
                group,
                is_custodial = (replica_entry['custodial'] == 'y'),
                last_update = time_update
            )

            block_replicas[site_name] = block_replica

            if replica_entry['complete'] == 'n':
                # temporarily make this a list
                block_replica.file_ids = []
                block_replica.size = 0
                LOG.info("Incomplete %s" % str(block_replica))

        if 'file' in block_entry:
            for file_entry in block_entry['file']:
                for replica_entry in file_entry['replica']:
                    site_name = replica_entry['node']
                    try:
                        block_replica = block_replicas[site_name]
                    except KeyError:
                        continue
    
                    if block_replica.file_ids is None:
                        continue
    
                    # add LFN instead of file id
                    block_replica.file_ids.append(file_entry['name'])
                    file_size = file_entry['bytes']
                    if file_size is not None:
                        block_replica.size += file_size
    
                    try:
                        time_create = int(replica_entry['time_create'])
                    except TypeError:
                        pass
                    else:
                        if time_create > block_replica.last_update:
                            block_replica.last_update = time_create

        for block_replica in block_replicas.itervalues():
            if block_replica.file_ids is not None:
                block_replica.file_ids = tuple(block_replica.file_ids)

        return block_replicas.values()

    @staticmethod
    def maker_deletions(block, block_entry, site_check = None):
        replicas = []

        for deletion_entry in block_entry['deletion']:
            if site_check and not site_check(deletion_entry['node']):
                continue

            block_replica = BlockReplica(block, Site(deletion_entry['node']), Group.null_group)

            replicas.append(block_replica)

        return replicas
