import time
import collections
import logging
import math
import MySQLdb

from dynamo.dataformat import Configuration
from dynamo.utils.interface.htc import HTCondor
from dynamo.utils.interface.mysql import MySQL

GlobalQueueJob = collections.namedtuple('GlobalQueueJob', ['queue_time', 'completion_time', 'nodes_total', 'nodes_done', 'nodes_failed', 'nodes_queued'])

LOG = logging.getLogger(__name__)

class GlobalQueueRequestHistory(object):
    """
    Sets one attr:
      request_weight:  float value
    """

    produces = ['request_weight']

    _default_config = None

    @staticmethod
    def set_default(config):
        GlobalQueueRequestHistory._default_config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            config = GlobalQueueRequestHistory._default_config

        self._store = MySQL(config.store)
        self._htcondor = HTCondor(config.get('htcondor', None))

        # Weight computation halflife constant (given in days in config)
        self.weight_halflife = config.get('weight_halflife', 4) * 3600. * 24.

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = value

    def load(self, inventory):
        records = self._get_stored_records(inventory)
        self._compute(inventory, records)

    def _get_stored_records(self, inventory):
        """
        Get the dataset request data from DB.
        @param inventory  DynamoInventory
        @return  {dataset: {jobid: GlobalQueueJob}}
        """

        # pick up requests that are less than 1 year old
        # old requests will be removed automatically next time the access information is saved from memory
        sql = 'SELECT d.`name`, r.`id`, UNIX_TIMESTAMP(r.`queue_time`), UNIX_TIMESTAMP(r.`completion_time`),'
        sql += ' r.`nodes_total`, r.`nodes_done`, r.`nodes_failed`, r.`nodes_queued` FROM `dataset_requests` AS r'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = r.`dataset_id`'
        sql += ' WHERE r.`queue_time` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY d.`id`, r.`queue_time`'

        all_requests = {}
        num_records = 0

        # little speedup by not repeating lookups for the same dataset
        current_dataset_name = ''
        dataset_exists = True
        for dataset_name, job_id, queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued in self._store.xquery(sql):
            num_records += 1

            if dataset_name == current_dataset_name:
                if not dataset_exists:
                    continue
            else:
                current_dataset_name = dataset_name

                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    dataset_exists = False
                    continue
                else:
                    dataset_exists = True

                requests = all_requests[dataset] = {}

            requests[job_id] = GlobalQueueJob(queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued)

        try:
            last_update = self._store.query('SELECT UNIX_TIMESTAMP(`dataset_requests_last_update`) FROM `system`', retries = 1)[0]
        except IndexError:
            last_update = 0

        LOG.info('Loaded %d dataset request data. Last update at %s UTC', num_records, time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(last_update)))

        return all_requests

    def _compute(self, inventory, all_requests):
        """
        Set the dataset request weight based on request list. Formula:
          w = Sum(exp(-t_i/T))
        where t_i is the time distance of the ith request from now. T is defined in the configuration.
        @param inventory     DynamoInventory
        @param all_requests  {dataset: {jobid: GlobalQueueJob}}
        """

        now = time.time()
        decay_constant = self.weight_halflife / math.log(2.)

        for dataset in inventory.datasets.itervalues():
            try:
                requests = all_requests[dataset]
            except KeyError:
                dataset.attr['request_weight'] = 0.
                continue

            weight = 0.
            for job in requests.itervalues():
                # first element of reqdata tuple is the queue time
                weight += math.exp((job.queue_time - now) / decay_constant)
            
            dataset.attr['request_weight'] = weight

    def update(self, inventory):
        try:
            try:
                last_update = self._store.query('SELECT UNIX_TIMESTAMP(`dataset_requests_last_update`) FROM `system`', retries = 1)[0]
            except IndexError:
                last_update = time.time() - 3600 * 24 # just go back by a day
                if not self._read_only:
                    self._store.query('INSERT INTO `system` VALUES ()')

            if not self._read_only:
                self._store.query('UPDATE `system` SET `dataset_requests_last_update` = NOW()', retries = 0, silent = True)

        except MySQLdb.OperationalError:
            # We have a read-only config
            self._read_only = True
            LOG.info('Cannot write to DB. Switching to self._read_only.')

        source_records = self._get_source_records(inventory, last_update)

        if not self._read_only:
            self._save_records(source_records)
            # remove old entries
            self._store.query('DELETE FROM `dataset_requests` WHERE `queue_time` < DATE_SUB(NOW(), INTERVAL 1 YEAR)')
            self._store.query('UPDATE `system` SET `dataset_requests_last_update` = NOW()')

    def _get_source_records(self, inventory, last_update):
        """
        Get the dataset request data from Global Queue schedd.
        @param inventory    DynamoInventory
        @param last_update  UNIX timestamp
        @return {dataset: {jobid: GlobalQueueJob}}
        """

        constraint = 'TaskType=?="ROOT" && !isUndefined(DESIRED_CMSDataset) && (QDate > {last_update} || CompletionDate > {last_update})'.format(last_update = last_update)

        attributes = ['DESIRED_CMSDataset', 'GlobalJobId', 'QDate', 'CompletionDate', 'DAG_NodesTotal', 'DAG_NodesDone', 'DAG_NodesFailed', 'DAG_NodesQueued']
        
        job_ads = self._htcondor.find_jobs(constraint = constraint, attributes = attributes)

        job_ads.sort(key = lambda a: a['DESIRED_CMSDataset'])

        all_requests = {}

        for ad in job_ads:
            dataset_name = ad['DESIRED_CMSDataset']

            try:
                dataset = inventory.datasets[dataset_name]
            except KeyError:
                continue

            if dataset not in all_requests:
                all_requests[dataset] = {}

            try:
                nodes_total = ad['DAG_NodesTotal']
                nodes_done = ad['DAG_NodesDone']
                nodes_failed = ad['DAG_NodesFailed']
                nodes_queued = ad['DAG_NodesQueued']
            except KeyError:
                nodes_total = 0
                nodes_done = 0
                nodes_failed = 0
                nodes_queued = 0

            all_requests[dataset][ad['GlobalJobId']] = GlobalQueueJob(
                ad['QDate'],
                ad['CompletionDate'],
                nodes_total,
                nodes_done,
                nodes_failed,
                nodes_queued
            )

        return all_requests

    def _save_records(self, records):
        """
        Save the newly fetched request records.
        @param records  {dataset: {jobid: GlobalQueueJob}}
        """

        dataset_id_map = {}
        self._store.make_map('datasets', records.iterkeys(), dataset_id_map, None)

        fields = ('id', 'dataset_id', 'queue_time', 'completion_time', 'nodes_total', 'nodes_done', 'nodes_failed', 'nodes_queued')

        data = []
        for dataset, dataset_request_list in records.iteritems():
            dataset_id = dataset_id_map[dataset]

            for job_id, (queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued) in dataset_request_list.iteritems():
                data.append((
                    job_id,
                    dataset_id,
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(queue_time)),
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(completion_time)) if completion_time > 0 else '0000-00-00 00:00:00',
                    nodes_total,
                    nodes_done,
                    nodes_failed,
                    nodes_queued
                ))

        self._store.insert_many('dataset_requests', fields, None, data, do_update = True)

