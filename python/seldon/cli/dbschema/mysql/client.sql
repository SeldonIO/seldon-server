-- MySQL dump 10.13  Distrib 5.5.41, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: testclient
-- ------------------------------------------------------
-- Server version	5.5.41-0ubuntu0.14.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `action_comment`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_comment` (
  `action_id` bigint(20) NOT NULL,
  `comment` text,
  PRIMARY KEY (`action_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `action_tags`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_tags` (
  `action_id` bigint(20) NOT NULL DEFAULT '0',
  `tag_id` smallint(6) NOT NULL DEFAULT '0',
  `tag` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`action_id`,`tag_id`),
  KEY `a_index` (`action_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `action_type`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_type` (
  `type_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(25) DEFAULT NULL,
  `weight` double DEFAULT NULL,
  `def_value` double DEFAULT NULL,
  `link_type` int(11) DEFAULT NULL,
  `semantic` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `actions`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `actions` (
  `action_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `item_id` bigint(20) NOT NULL,
  `type` int(11) DEFAULT NULL,
  `times` int(11) DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `value` double DEFAULT NULL,
  `client_user_id` varchar(255) DEFAULT NULL,
  `client_item_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`action_id`),
  KEY `i_index` (`item_id`),
  KEY `idx` (`user_id`,`item_id`),
  KEY `cukey` (`client_user_id`),
  KEY `cikey` (`client_item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (action_id)
(PARTITION p1 VALUES LESS THAN (10000000) ENGINE = InnoDB,
 PARTITION p2 VALUES LESS THAN (20000000) ENGINE = InnoDB,
 PARTITION p3 VALUES LESS THAN (30000000) ENGINE = InnoDB,
 PARTITION p4 VALUES LESS THAN (40000000) ENGINE = InnoDB,
 PARTITION p5 VALUES LESS THAN (50000000) ENGINE = InnoDB,
 PARTITION p6 VALUES LESS THAN (60000000) ENGINE = InnoDB,
 PARTITION p7 VALUES LESS THAN (70000000) ENGINE = InnoDB,
 PARTITION p8 VALUES LESS THAN (80000000) ENGINE = InnoDB,
 PARTITION p9 VALUES LESS THAN (90000000) ENGINE = InnoDB,
 PARTITION p10 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_attr_exclude`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_attr_exclude` (
  `attr_id` int(11) NOT NULL,
  PRIMARY KEY (`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_counts`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_counts` (
  `id` int(11) NOT NULL,
  `item_id` bigint(20) NOT NULL,
  `count` double NOT NULL,
  `t` bigint(20) NOT NULL,
  PRIMARY KEY (`id`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_counts_item_total`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_counts_item_total` (
  `item_id` bigint(20) NOT NULL,
  `total` double NOT NULL,
  `t` bigint(20) NOT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_counts_total`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_counts_total` (
  `uid` int(11) NOT NULL,
  `total` double NOT NULL,
  `t` bigint(20) NOT NULL,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_dim_exclude`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_dim_exclude` (
  `dim_id` int(11) NOT NULL,
  PRIMARY KEY (`dim_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_group`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_group` (
  `cluster_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_referrer`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_referrer` (
  `referrer` varchar(255) NOT NULL,
  `cluster` int(11) NOT NULL,
  PRIMARY KEY (`referrer`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_update`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_update` (
  `lastupdate` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `demographic`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `demographic` (
  `demo_id` int(11) NOT NULL AUTO_INCREMENT,
  `attr_id` int(11) DEFAULT NULL,
  `value_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`demo_id`),
  KEY `idx` (`attr_id`,`value_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dimension`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dimension` (
  `dim_id` int(11) NOT NULL AUTO_INCREMENT,
  `item_type` int(11) DEFAULT NULL,
  `attr_id` int(11) DEFAULT NULL,
  `value_id` int(11) DEFAULT NULL,
  `trustnetwork` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`dim_id`),
  KEY `idx` (`attr_id`,`value_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr` (
  `attr_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(25) DEFAULT NULL,
  `type` varchar(10) DEFAULT NULL,
  `item_type` int(11) DEFAULT NULL,
  `semantic` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`attr_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_bigint`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_bigint` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` bigint(20) DEFAULT NULL,
  `max` int(11) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_boolean`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_boolean` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` tinyint(1) DEFAULT NULL,
  `max` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_datetime`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_datetime` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` datetime DEFAULT NULL,
  `max` datetime DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_double`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_double` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` double DEFAULT NULL,
  `max` int(11) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_enum`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_enum` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `value_name` varchar(255) DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `name` bit(1) DEFAULT b'0',
  PRIMARY KEY (`attr_id`,`value_id`),
  KEY `a_index` (`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_int`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_int` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` int(11) DEFAULT NULL,
  `max` int(11) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_attr_varchar`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_attr_varchar` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` varchar(255) DEFAULT NULL,
  `max` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_clusters`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_clusters` (
  `item_id` bigint(20) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `weight` double NOT NULL,
  `created` bigint(20) NOT NULL,
  PRIMARY KEY (`item_id`,`cluster_id`),
  KEY `timekey` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_clusters_new`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_clusters_new` (
  `item_id` bigint(20) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `weight` double NOT NULL,
  `created` bigint(20) NOT NULL,
  PRIMARY KEY (`item_id`,`cluster_id`),
  KEY `timekey` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_counts`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_counts` (
  `item_id` bigint(20) NOT NULL,
  `count` int(11) NOT NULL,
  `time` bigint(20) NOT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_demo`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_demo` (
  `item_id` bigint(20) NOT NULL,
  `demo_id` int(11) NOT NULL,
  `relevance` double DEFAULT NULL,
  PRIMARY KEY (`item_id`,`demo_id`),
  KEY `u_index` (`item_id`),
  KEY `d_index` (`demo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_bigint`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_bigint` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_boolean`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_boolean` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_datetime`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_datetime` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` datetime DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_double`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_double` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` double DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_enum`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_enum` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `i_index` (`item_id`),
  KEY `a_index` (`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_int`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_int` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` int(11) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_text`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_text` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` text,
  PRIMARY KEY (`attr_id`,`item_id`),
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_map_varchar`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_map_varchar` (
  `item_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` varchar(255) DEFAULT NULL,
  `pos` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`attr_id`,`item_id`,`pos`) USING BTREE,
  KEY `uidx` (`item_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_similarity`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_similarity` (
  `item_id` bigint(20) NOT NULL,
  `item_id2` bigint(20) NOT NULL,
  `score` double NOT NULL,
  PRIMARY KEY (`item_id`,`item_id2`),
  UNIQUE KEY `i2` (`item_id2`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_similarity_new`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_similarity_new` (
  `item_id` bigint(20) NOT NULL,
  `item_id2` bigint(20) NOT NULL,
  `score` double NOT NULL,
  PRIMARY KEY (`item_id`,`item_id2`),
  UNIQUE KEY `i2` (`item_id2`,`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_type`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item_type` (
  `type_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(25) DEFAULT NULL,
  `link_type` int(11) DEFAULT NULL,
  `semantic` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `items`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `items` (
  `item_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `first_op` datetime DEFAULT NULL,
  `last_op` datetime DEFAULT NULL,
  `popular` tinyint(1) DEFAULT '0',
  `client_item_id` varchar(255) DEFAULT NULL,
  `type` int(11) DEFAULT '0',
  `avgrating` double DEFAULT '0',
  `stddevrating` double DEFAULT '0',
  `num_op` int(11) DEFAULT NULL,
  PRIMARY KEY (`item_id`),
  UNIQUE KEY `c_index` (`client_item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `items_popular`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `items_popular` (
  `item_id` bigint(20) NOT NULL,
  `opsum` double DEFAULT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `items_popular_new`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `items_popular_new` (
  `item_id` bigint(20) NOT NULL,
  `opsum` double DEFAULT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr` (
  `attr_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(25) DEFAULT NULL,
  `type` varchar(10) DEFAULT NULL,
  `link_type` int(11) DEFAULT NULL,
  `demographic` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`attr_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_bigint`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_bigint` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` bigint(20) DEFAULT NULL,
  `max` int(11) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_boolean`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_boolean` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` tinyint(1) DEFAULT NULL,
  `max` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_datetime`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_datetime` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` datetime DEFAULT NULL,
  `max` datetime DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_double`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_double` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` double DEFAULT NULL,
  `max` int(11) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_enum`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_enum` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `value_name` varchar(50) DEFAULT NULL,
  `amount` double DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`value_id`),
  KEY `a_index` (`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_int`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_int` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` int(11) DEFAULT NULL,
  `max` int(11) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_attr_varchar`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attr_varchar` (
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) NOT NULL,
  `min` varchar(25) DEFAULT NULL,
  `max` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`value_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_clusters`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_clusters` (
  `user_id` bigint(20) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `weight` double NOT NULL,
  PRIMARY KEY (`user_id`,`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_clusters_new`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_clusters_new` (
  `user_id` bigint(20) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `weight` double NOT NULL,
  PRIMARY KEY (`user_id`,`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_clusters_transient`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_clusters_transient` (
  `t_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `cluster_id` int(11) NOT NULL,
  `weight` double NOT NULL,
  PRIMARY KEY (`t_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_dim`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_dim` (
  `user_id` bigint(20) NOT NULL,
  `dim_id` int(11) NOT NULL,
  `relevance` double DEFAULT NULL,
  PRIMARY KEY (`user_id`,`dim_id`),
  KEY `u_index` (`user_id`),
  KEY `d_index` (`dim_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_dim_new`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_dim_new` (
  `user_id` bigint(20) NOT NULL,
  `dim_id` int(11) NOT NULL,
  `relevance` double DEFAULT NULL,
  PRIMARY KEY (`user_id`,`dim_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_interaction`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_interaction` (
  `u1_id` bigint(20) unsigned NOT NULL,
  `u2_fbid` varchar(30) NOT NULL,
  `type` int(11) NOT NULL,
  `sub_type` int(11) NOT NULL,
  `count` int(11) NOT NULL,
  `parameter_id` int(11) NOT NULL DEFAULT '0',
  `date` datetime DEFAULT NULL,
  PRIMARY KEY (`u1_id`,`u2_fbid`,`type`,`sub_type`,`parameter_id`),
  KEY `u1idx` (`u1_id`),
  KEY `u1typeidx` (`u1_id`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_bigint`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_bigint` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_boolean`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_boolean` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_datetime`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_datetime` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` datetime DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_double`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_double` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` double DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_enum`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_enum` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `i_index` (`user_id`),
  KEY `a_index` (`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_int`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_int` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` int(11) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_text`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_text` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` text,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_map_varchar`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_map_varchar` (
  `user_id` bigint(20) NOT NULL,
  `attr_id` int(11) NOT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`attr_id`,`user_id`),
  KEY `uidx` (`user_id`,`attr_id`),
  KEY `avidx` (`attr_id`,`value`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_similarity`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_similarity` (
  `u1` bigint(20) NOT NULL,
  `u2` bigint(20) NOT NULL,
  `type` int(11) NOT NULL,
  `score` double NOT NULL,
  PRIMARY KEY (`u1`,`u2`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_tag`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_tag` (
  `user_id` bigint(20) NOT NULL,
  `tag` varchar(255) NOT NULL,
  `count` int(11) NOT NULL,
  PRIMARY KEY (`user_id`,`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_user_avg`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_user_avg` (
  `m1` bigint(20) NOT NULL,
  `m2` bigint(20) NOT NULL,
  `avgm1` double DEFAULT NULL,
  `avgm2` double DEFAULT NULL,
  PRIMARY KEY (`m1`,`m2`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `user_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `username` varchar(64) DEFAULT NULL,
  `first_op` datetime DEFAULT NULL,
  `last_op` datetime DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `num_op` int(11) DEFAULT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `client_user_id` varchar(255) DEFAULT NULL,
  `avgrating` double DEFAULT '0',
  `stddevrating` double DEFAULT '0',
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `c_index` (`client_user_id`),
  KEY `u_index` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `version`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `version` (
  `major` int(11) NOT NULL,
  `minor` int(11) NOT NULL,
  `bugfix` int(11) NOT NULL,
  `date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

--
-- Table structure for table `items_recent_popularity`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `items_recent_popularity` (
  `item_id` int(11) unsigned NOT NULL,
  `score` float DEFAULT '0',
  `decay_id` int(11) NOT NULL DEFAULT '0',
  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`item_id`,`decay_id`),
  KEY `score` (`score`,`decay_id`),
  KEY `decay_id` (`decay_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

-- Dump completed on 2015-03-03 10:42:31
