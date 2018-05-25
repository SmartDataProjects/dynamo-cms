<?php

function check_cache($history_db, $cycle, $partition_id, $snapshot_spool_path, $snapshot_archive_path)
{
  $stmt = $history_db->prepare('SELECT DATABASE()');
  $stmt->bind_result($db_name);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  $cache_db_name = $db_name . "_cache";
  $replica_cache_table_name = "replicas_" . $cycle;
  $site_cache_table_name = "sites_" . $cycle;

  $stmt = $history_db->prepare('SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?');
  $stmt->bind_param('ss', $cache_db_name, $replica_cache_table_name);
  $stmt->bind_result($num_replica_table);
  $stmt->execute();
  $stmt->fetch();

  $stmt->bind_param('ss', $cache_db_name, $site_cache_table_name);
  $stmt->bind_result($num_site_table);
  $stmt->execute();
  $stmt->fetch();

  $stmt->close();

  if ($num_replica_table == 0 || $num_site_table == 0) {
    $sqlite_file_name = sprintf('%s/snapshot_%09d.db', $snapshot_spool_path, $cycle);

    if (!file_exists($sqlite_file_name)) {
      $srun = sprintf('%09d', $cycle);
      $xz_file_name = sprintf('%s/%s/%s/snapshot_%09d.db.xz', $snapshot_archive_path, substr($srun, 0, 3), substr($srun, 3, 3), $cycle);
      if (!file_exists($xz_file_name))
        return false;

      exec('which unxz > /dev/null 2>&1', $stdout, $rc);
      if ($rc != 0)
        return false;
      
      exec(sprintf('unxz -k -c %s > %s', $xz_file_name, $sqlite_file_name));
      chmod($sqlite_file_name, 0666);
    }

    $snapshot_db = new SQLite3($sqlite_file_name);

    if ($num_replica_table == 0) {
      $sql = 'SELECT r.`site_id`, r.`dataset_id`, r.`size`, d.`value`, r.`condition` FROM `replicas` AS r';
      $sql .= ' INNER JOIN `decisions` AS d ON d.`id` = r.`decision_id`';
      $in_stmt = $snapshot_db->prepare($sql);
      $result = $in_stmt->execute();

      $history_db->query(sprintf('CREATE TABLE `%s`.`%s` LIKE `%s`.`replicas`', $cache_db_name, $replica_cache_table_name, $cache_db_name));

      $stmt = $history_db->prepare(sprintf('INSERT INTO `%s`.`%s` VALUES (?, ?, ?, ?, ?)', $cache_db_name, $replica_cache_table_name));
      $stmt->bind_param('iiisi', $site_id, $dataset_id, $size, $decision, $condition);

      while (($arr = $result->fetchArray(SQLITE3_NUM))) {
        $site_id = $arr[0];
        $dataset_id = $arr[1];
        $size = $arr[2];
        $decision = $arr[3];
        $condition = $arr[4];

        $stmt->execute();
      }

      $in_stmt->close();
      $stmt->close();
    }

    if ($num_site_table == 0) {
      $sql = 'SELECT n.`site_id`, s.`value`, n.`quota` FROM `sites` AS n';
      $sql .= ' INNER JOIN `statuses` AS s ON s.`id` = n.`status_id`';
      $in_stmt = $snapshot_db->prepare($sql);
      $result = $in_stmt->execute();

      $history_db->query(sprintf('CREATE TABLE `%s`.`%s` LIKE `%s`.`sites`', $cache_db_name, $site_cache_table_name, $cache_db_name));

      $stmt = $history_db->prepare(sprintf('INSERT INTO `%s`.`%s` VALUES (?, ?, ?)', $cache_db_name, $site_cache_table_name));
      $stmt->bind_param('isi', $site_id, $status, $quota);

      while (($arr = $result->fetchArray(SQLITE3_NUM))) {
        $site_id = $arr[0];
        $status = $arr[1];
        $quota = $arr[2];

        $stmt->execute();
      }

      $in_stmt->close();
      $stmt->close();
    }

    $snapshot_db->close();
  }

  $stmt = $history_db->prepare('INSERT INTO `' . $cache_db_name . '`.`replicas_snapshot_usage` VALUES (?, NOW())');
  $stmt->bind_param('i', $cycle);
  $stmt->execute();
  $stmt->close();

  $stmt = $history_db->prepare('INSERT INTO `' . $cache_db_name . '`.`sites_snapshot_usage` VALUES (?, NOW())');
  $stmt->bind_param('i', $cycle);
  $stmt->execute();
  $stmt->close();

  return true;
}

?>
