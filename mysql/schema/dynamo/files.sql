CREATE TABLE `files` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `block_id` bigint(20) unsigned NOT NULL DEFAULT '0',
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `crc32` int(11) unsigned NOT NULL,
  `adler32` varchar(8) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `blocks` (`block_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
