/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19  Distrib 10.11.14-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: weblog
-- ------------------------------------------------------
-- Server version	10.11.14-MariaDB-0ubuntu0.24.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `ai_incident_summary_day`
--

DROP TABLE IF EXISTS `ai_incident_summary_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ai_incident_summary_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `run_id` varchar(64) NOT NULL,
  `risk_score` decimal(20,6) DEFAULT NULL,
  `actual_risk_status` varchar(20) DEFAULT NULL,
  `predicted_risk_status` varchar(20) DEFAULT NULL,
  `predicted_alert_prob` decimal(20,6) DEFAULT NULL,
  `incident_title` varchar(255) DEFAULT NULL,
  `incident_level` varchar(20) DEFAULT NULL,
  `executive_summary` text DEFAULT NULL,
  `technical_summary` text DEFAULT NULL,
  `business_impact` text DEFAULT NULL,
  `recommended_actions` text DEFAULT NULL,
  `confidence_score` decimal(20,6) DEFAULT NULL,
  `llm_model` varchar(100) DEFAULT NULL,
  `prompt_version` varchar(50) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_prompt_log`
--

DROP TABLE IF EXISTS `ai_prompt_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ai_prompt_log` (
  `run_id` varchar(64) NOT NULL,
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `prompt_version` varchar(50) NOT NULL,
  `llm_model` varchar(100) NOT NULL,
  `prompt_text` longtext DEFAULT NULL,
  `response_text` longtext DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_recommended_action_day`
--

DROP TABLE IF EXISTS `ai_recommended_action_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ai_recommended_action_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `action_rank` int(11) NOT NULL,
  `action_type` varchar(50) DEFAULT NULL,
  `action_title` varchar(255) DEFAULT NULL,
  `action_detail` text DEFAULT NULL,
  `owner_hint` varchar(100) DEFAULT NULL,
  `priority` varchar(20) DEFAULT NULL,
  `evidence` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`action_rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_reliability_action_day`
--

DROP TABLE IF EXISTS `data_reliability_action_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_reliability_action_day` (
  `dt` date NOT NULL,
  `metric_nm` varchar(100) NOT NULL,
  `root_cause` varchar(100) NOT NULL,
  `action_type` varchar(100) NOT NULL,
  `priority` varchar(20) NOT NULL,
  `confidence` decimal(8,4) NOT NULL DEFAULT 0.0000,
  `recommended_fix` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`dt`,`metric_nm`,`root_cause`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_risk_root_cause_day`
--

DROP TABLE IF EXISTS `data_risk_root_cause_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_risk_root_cause_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `cause_rank` int(11) NOT NULL,
  `cause_type` varchar(50) NOT NULL,
  `cause_code` varchar(100) NOT NULL,
  `confidence` decimal(8,4) NOT NULL DEFAULT 0.0000,
  `driver_source` varchar(50) NOT NULL,
  `related_metric` varchar(100) DEFAULT NULL,
  `observed_value` decimal(20,6) DEFAULT NULL,
  `baseline_value` decimal(20,6) DEFAULT NULL,
  `detail` varchar(255) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`cause_rank`),
  KEY `idx_rca_profile_dt` (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_risk_score_day`
--

DROP TABLE IF EXISTS `data_risk_score_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_risk_score_day` (
  `profile_id` varchar(100) NOT NULL,
  `dt` date NOT NULL,
  `validation_fail_count` int(11) NOT NULL,
  `validation_warn_count` int(11) NOT NULL,
  `drift_alert_count` int(11) NOT NULL,
  `drift_warn_count` int(11) NOT NULL,
  `risk_score` int(11) NOT NULL,
  `risk_status` varchar(20) NOT NULL,
  `run_id` varchar(100) NOT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_risk_score_day_v2`
--

DROP TABLE IF EXISTS `data_risk_score_day_v2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_risk_score_day_v2` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `validation_fail_count` int(11) NOT NULL DEFAULT 0,
  `validation_warn_count` int(11) NOT NULL DEFAULT 0,
  `drift_alert_count` int(11) NOT NULL DEFAULT 0,
  `drift_warn_count` int(11) NOT NULL DEFAULT 0,
  `ml_feature_alert_count` int(11) NOT NULL DEFAULT 0,
  `ml_feature_warn_count` int(11) NOT NULL DEFAULT 0,
  `risk_score` decimal(18,6) NOT NULL DEFAULT 0.000000,
  `risk_status` varchar(20) NOT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_risk_score_day_v3`
--

DROP TABLE IF EXISTS `data_risk_score_day_v3`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_risk_score_day_v3` (
  `profile_id` varchar(100) NOT NULL,
  `dt` date NOT NULL,
  `metric_nm` varchar(100) NOT NULL,
  `validation_score` decimal(10,4) NOT NULL DEFAULT 0.0000,
  `drift_score` decimal(10,4) NOT NULL DEFAULT 0.0000,
  `anomaly_score` decimal(10,4) NOT NULL DEFAULT 0.0000,
  `mapping_score` decimal(10,4) NOT NULL DEFAULT 0.0000,
  `final_risk_score` decimal(10,4) NOT NULL DEFAULT 0.0000,
  `risk_grade` varchar(20) NOT NULL DEFAULT 'low',
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`metric_nm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `event_mapping`
--

DROP TABLE IF EXISTS `event_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `event_mapping` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `domain` varchar(100) NOT NULL DEFAULT 'default',
  `url_pattern` varchar(500) NOT NULL,
  `method` varchar(10) DEFAULT 'GET',
  `event_name` varchar(100) NOT NULL,
  `event_type` varchar(50) DEFAULT 'page',
  `funnel_stage` varchar(50) DEFAULT NULL,
  `funnel_order` int(11) DEFAULT 0,
  `category` varchar(100) DEFAULT NULL,
  `subcategory` varchar(100) DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT 1,
  `description` varchar(500) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pattern` (`domain`,`url_pattern`,`method`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `log_format_meta`
--

DROP TABLE IF EXISTS `log_format_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `log_format_meta` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `site_key` varchar(64) NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT 1,
  `template` text NOT NULL,
  `http_version` varchar(16) NOT NULL DEFAULT 'HTTP/1.1',
  `tz_offset_min` int(11) NOT NULL DEFAULT 540,
  `note` varchar(255) DEFAULT NULL,
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_site_key` (`site_key`),
  KEY `idx_site_key_enabled` (`site_key`,`enabled`),
  KEY `idx_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mapping_coverage_day`
--

DROP TABLE IF EXISTS `mapping_coverage_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `mapping_coverage_day` (
  `dt` date NOT NULL,
  `domain` varchar(100) NOT NULL DEFAULT 'default',
  `total_events` bigint(20) NOT NULL DEFAULT 0,
  `mapped_events` bigint(20) NOT NULL DEFAULT 0,
  `unmapped_events` bigint(20) NOT NULL DEFAULT 0,
  `mapping_coverage` decimal(8,4) NOT NULL DEFAULT 0.0000,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`dt`,`domain`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_batch_run`
--

DROP TABLE IF EXISTS `metric_batch_run`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_batch_run` (
  `run_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `profile_id` varchar(50) NOT NULL,
  `dt_from` date NOT NULL,
  `dt_to` date NOT NULL,
  `started_at` datetime NOT NULL,
  `finished_at` datetime DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'running',
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`run_id`),
  KEY `idx_profile_dt` (`profile_id`,`dt_from`,`dt_to`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_correlation_anomaly_day`
--

DROP TABLE IF EXISTS `metric_correlation_anomaly_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_correlation_anomaly_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `pair_name` varchar(150) NOT NULL,
  `left_metric` varchar(100) NOT NULL,
  `right_metric` varchar(100) NOT NULL,
  `baseline_ratio` decimal(20,6) DEFAULT NULL,
  `observed_ratio` decimal(20,6) DEFAULT NULL,
  `ratio_diff` decimal(20,6) DEFAULT NULL,
  `ratio_diff_pct` decimal(20,6) DEFAULT NULL,
  `anomaly_status` varchar(20) NOT NULL,
  `severity` varchar(20) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`pair_name`),
  KEY `idx_corr_anom_profile_dt` (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_definition`
--

DROP TABLE IF EXISTS `metric_definition`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_definition` (
  `metric_name` varchar(100) NOT NULL,
  `metric_group` varchar(50) NOT NULL,
  `source_layer` varchar(30) NOT NULL,
  `agg_level` varchar(10) NOT NULL,
  `unit` varchar(30) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `is_ratio` tinyint(1) NOT NULL DEFAULT 0,
  `is_enabled` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`metric_name`,`source_layer`,`agg_level`),
  KEY `idx_metric_group` (`metric_group`),
  KEY `idx_enabled` (`is_enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_drift_result`
--

DROP TABLE IF EXISTS `metric_drift_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_drift_result` (
  `drift_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `profile_id` varchar(50) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(3) unsigned DEFAULT NULL,
  `metric_name` varchar(100) NOT NULL,
  `metric_group` varchar(50) NOT NULL,
  `source_layer` varchar(30) NOT NULL,
  `baseline_value` decimal(20,6) DEFAULT NULL,
  `observed_value` decimal(20,6) NOT NULL,
  `drift_score` decimal(20,6) DEFAULT NULL,
  `drift_method` varchar(50) NOT NULL,
  `drift_status` varchar(20) NOT NULL,
  `severity` varchar(20) NOT NULL DEFAULT 'info',
  `run_id` bigint(20) unsigned DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `detail` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`drift_id`),
  KEY `idx_profile_dt` (`profile_id`,`dt`),
  KEY `idx_metric` (`metric_name`),
  KEY `idx_status` (`drift_status`),
  KEY `idx_run_id` (`run_id`)
) ENGINE=InnoDB AUTO_INCREMENT=56341 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_drift_result_r`
--

DROP TABLE IF EXISTS `metric_drift_result_r`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_drift_result_r` (
  `profile_id` varchar(100) NOT NULL,
  `dt` date NOT NULL,
  `hh` smallint(6) DEFAULT NULL,
  `metric_name` varchar(100) NOT NULL,
  `baseline_value` decimal(18,6) DEFAULT NULL,
  `observed_value` decimal(18,6) DEFAULT NULL,
  `drift_score` decimal(18,6) DEFAULT NULL,
  `drift_method` varchar(50) NOT NULL,
  `drift_status` varchar(20) NOT NULL,
  `severity` varchar(20) DEFAULT NULL,
  `detail` text DEFAULT NULL,
  `run_id` varchar(100) NOT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_time_anomaly_day`
--

DROP TABLE IF EXISTS `metric_time_anomaly_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_time_anomaly_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `metric_name` varchar(100) NOT NULL,
  `metric_group` varchar(50) DEFAULT NULL,
  `source_layer` varchar(50) DEFAULT NULL,
  `observed_value` decimal(20,6) DEFAULT NULL,
  `rolling_avg_7d` decimal(20,6) DEFAULT NULL,
  `rolling_std_7d` decimal(20,6) DEFAULT NULL,
  `zscore_7d` decimal(20,6) DEFAULT NULL,
  `anomaly_status` varchar(20) NOT NULL,
  `severity` varchar(20) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`metric_name`),
  KEY `idx_time_anom_profile_dt` (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_validation_result`
--

DROP TABLE IF EXISTS `metric_validation_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_validation_result` (
  `validation_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `profile_id` varchar(50) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(3) unsigned DEFAULT NULL,
  `rule_name` varchar(100) NOT NULL,
  `metric_name` varchar(100) DEFAULT NULL,
  `source_layer` varchar(30) DEFAULT NULL,
  `validation_status` varchar(20) NOT NULL,
  `observed_value` decimal(20,6) DEFAULT NULL,
  `expected_value` decimal(20,6) DEFAULT NULL,
  `diff_value` decimal(20,6) DEFAULT NULL,
  `severity` varchar(20) NOT NULL DEFAULT 'info',
  `run_id` bigint(20) unsigned DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`validation_id`),
  KEY `idx_profile_dt` (`profile_id`,`dt`),
  KEY `idx_rule` (`rule_name`),
  KEY `idx_status` (`validation_status`),
  KEY `idx_run_id` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_value_day`
--

DROP TABLE IF EXISTS `metric_value_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_value_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `metric_name` varchar(100) NOT NULL,
  `metric_group` varchar(50) NOT NULL,
  `source_layer` varchar(50) NOT NULL,
  `metric_value` decimal(18,6) NOT NULL,
  `numerator_value` decimal(18,6) DEFAULT NULL,
  `denominator_value` decimal(18,6) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`metric_name`),
  KEY `idx_metric_value_day_1` (`dt`,`metric_name`),
  KEY `idx_metric_value_day_2` (`profile_id`,`metric_name`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metric_value_hh`
--

DROP TABLE IF EXISTS `metric_value_hh`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metric_value_hh` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(4) NOT NULL,
  `metric_name` varchar(100) NOT NULL,
  `metric_group` varchar(50) NOT NULL,
  `source_layer` varchar(50) NOT NULL,
  `metric_value` decimal(18,6) NOT NULL,
  `numerator_value` decimal(18,6) DEFAULT NULL,
  `denominator_value` decimal(18,6) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`hh`,`metric_name`),
  KEY `idx_metric_value_hh_1` (`dt`,`hh`,`metric_name`),
  KEY `idx_metric_value_hh_2` (`profile_id`,`metric_name`,`dt`,`hh`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ml_feature_drift_result`
--

DROP TABLE IF EXISTS `ml_feature_drift_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ml_feature_drift_result` (
  `feature_drift_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(4) DEFAULT NULL,
  `feature_name` varchar(100) NOT NULL,
  `feature_group` varchar(50) NOT NULL,
  `baseline_value` decimal(20,6) DEFAULT NULL,
  `observed_value` decimal(20,6) DEFAULT NULL,
  `baseline_sd` decimal(20,6) DEFAULT NULL,
  `drift_score` decimal(20,6) DEFAULT NULL,
  `drift_method` varchar(50) NOT NULL,
  `drift_status` varchar(20) NOT NULL,
  `severity` varchar(20) NOT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`feature_drift_id`),
  KEY `idx_profile_dt` (`profile_id`,`dt`),
  KEY `idx_feature` (`feature_name`),
  KEY `idx_status` (`drift_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ml_feature_importance`
--

DROP TABLE IF EXISTS `ml_feature_importance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ml_feature_importance` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `model_name` varchar(100) NOT NULL,
  `model_version` varchar(64) NOT NULL,
  `feature_name` varchar(100) NOT NULL,
  `coefficient` decimal(20,10) DEFAULT NULL,
  `abs_coefficient` decimal(20,10) DEFAULT NULL,
  `importance_mean` decimal(20,10) DEFAULT NULL,
  `importance_std` decimal(20,10) DEFAULT NULL,
  `importance_rank` int(11) NOT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`model_name`,`model_version`,`feature_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ml_feature_vector_day`
--

DROP TABLE IF EXISTS `ml_feature_vector_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ml_feature_vector_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `daily_active_users` decimal(20,6) DEFAULT NULL,
  `page_view_count` decimal(20,6) DEFAULT NULL,
  `avg_session_duration_sec` decimal(20,6) DEFAULT NULL,
  `new_user_ratio` decimal(20,6) DEFAULT NULL,
  `auth_attempt_count` decimal(20,6) DEFAULT NULL,
  `auth_success_count` decimal(20,6) DEFAULT NULL,
  `auth_fail_count` decimal(20,6) DEFAULT NULL,
  `auth_success_rate` decimal(20,6) DEFAULT NULL,
  `auth_fail_rate` decimal(20,6) DEFAULT NULL,
  `otp_request_count` decimal(20,6) DEFAULT NULL,
  `risk_login_count` decimal(20,6) DEFAULT NULL,
  `loan_view_count` decimal(20,6) DEFAULT NULL,
  `loan_apply_start_count` decimal(20,6) DEFAULT NULL,
  `loan_apply_submit_count` decimal(20,6) DEFAULT NULL,
  `card_apply_start_count` decimal(20,6) DEFAULT NULL,
  `card_apply_submit_count` decimal(20,6) DEFAULT NULL,
  `card_apply_submit_rate` decimal(20,6) DEFAULT NULL,
  `collector_event_count` decimal(20,6) DEFAULT NULL,
  `raw_event_count` decimal(20,6) DEFAULT NULL,
  `estimated_missing_rate` decimal(20,6) DEFAULT NULL,
  `validation_fail_count` int(11) NOT NULL DEFAULT 0,
  `validation_warn_count` int(11) NOT NULL DEFAULT 0,
  `drift_alert_count` int(11) NOT NULL DEFAULT 0,
  `drift_warn_count` int(11) NOT NULL DEFAULT 0,
  `ml_feature_alert_count` int(11) NOT NULL DEFAULT 0,
  `ml_feature_warn_count` int(11) NOT NULL DEFAULT 0,
  `target_risk_status` varchar(20) DEFAULT NULL,
  `target_risk_score` decimal(20,6) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `target_risk_label` int(11) DEFAULT NULL,
  `label_source` varchar(100) DEFAULT NULL,
  `scenario_active_flag` tinyint(4) DEFAULT NULL,
  `scenario_name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ml_prediction_result`
--

DROP TABLE IF EXISTS `ml_prediction_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ml_prediction_result` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `model_name` varchar(100) NOT NULL,
  `model_version` varchar(64) NOT NULL,
  `predicted_label` tinyint(4) DEFAULT NULL,
  `predicted_risk_status` varchar(20) NOT NULL,
  `prob_normal` decimal(20,6) DEFAULT NULL,
  `prob_warning` decimal(20,6) DEFAULT NULL,
  `prob_alert` decimal(20,6) DEFAULT NULL,
  `actual_risk_status` varchar(20) DEFAULT NULL,
  `actual_risk_score` decimal(20,6) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`model_name`,`model_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `risk_signal_link_day`
--

DROP TABLE IF EXISTS `risk_signal_link_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `risk_signal_link_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `signal_group` varchar(50) NOT NULL,
  `signal_name` varchar(100) NOT NULL,
  `signal_count` int(11) NOT NULL DEFAULT 0,
  `weighted_contribution` decimal(20,6) NOT NULL DEFAULT 0.000000,
  `severity` varchar(20) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`profile_id`,`dt`,`signal_group`,`signal_name`),
  KEY `idx_risk_link_profile_dt` (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `scenario_experiment_result_day`
--

DROP TABLE IF EXISTS `scenario_experiment_result_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `scenario_experiment_result_day` (
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `scenario_run_id` bigint(20) unsigned NOT NULL,
  `scenario_name` varchar(100) NOT NULL,
  `scenario_type` varchar(50) NOT NULL,
  `risk_score_v2` decimal(20,6) DEFAULT NULL,
  `risk_score_v3` decimal(20,6) DEFAULT NULL,
  `validation_warn_count` int(11) DEFAULT NULL,
  `validation_fail_count` int(11) DEFAULT NULL,
  `drift_alert_count` int(11) DEFAULT NULL,
  `drift_warn_count` int(11) DEFAULT NULL,
  `ml_feature_alert_count` int(11) DEFAULT NULL,
  `ml_feature_warn_count` int(11) DEFAULT NULL,
  `predicted_alert_prob` decimal(20,6) DEFAULT NULL,
  `predicted_label` varchar(30) DEFAULT NULL,
  `root_cause_top1` varchar(255) DEFAULT NULL,
  `traffic_page_view_count` decimal(20,6) DEFAULT NULL,
  `missing_rate` decimal(20,6) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `label_source` varchar(50) DEFAULT NULL,
  `prediction_mode` varchar(30) DEFAULT NULL,
  `scenario_active_flag` tinyint(4) DEFAULT NULL,
  `target_risk_label` int(11) DEFAULT NULL,
  PRIMARY KEY (`profile_id`,`dt`,`scenario_run_id`),
  KEY `idx_scenario_result_profile_dt` (`profile_id`,`dt`),
  KEY `idx_scenario_result_run` (`scenario_run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `scenario_experiment_run`
--

DROP TABLE IF EXISTS `scenario_experiment_run`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `scenario_experiment_run` (
  `scenario_run_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `profile_id` varchar(64) NOT NULL,
  `scenario_name` varchar(100) NOT NULL,
  `scenario_type` varchar(50) NOT NULL,
  `dt_from` date NOT NULL,
  `dt_to` date NOT NULL,
  `parameters_json` text DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `started_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `scenario_severity` varchar(20) DEFAULT NULL,
  `scenario_intensity` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`scenario_run_id`),
  KEY `idx_scenario_run_profile_dt` (`profile_id`,`dt_from`,`dt_to`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `scenario_metric_change_log`
--

DROP TABLE IF EXISTS `scenario_metric_change_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `scenario_metric_change_log` (
  `scenario_run_id` bigint(20) unsigned NOT NULL,
  `profile_id` varchar(64) NOT NULL,
  `dt` date NOT NULL,
  `metric_name` varchar(100) NOT NULL,
  `before_value` decimal(20,6) DEFAULT NULL,
  `after_value` decimal(20,6) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `scenario_name` varchar(100) DEFAULT NULL,
  `scenario_type` varchar(50) DEFAULT NULL,
  `scenario_intensity` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`scenario_run_id`,`profile_id`,`dt`,`metric_name`),
  KEY `idx_scenario_metric_change_dt` (`profile_id`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stg_ds_metric`
--

DROP TABLE IF EXISTS `stg_ds_metric`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `stg_ds_metric` (
  `profile_id` varchar(20) NOT NULL,
  `dt` date NOT NULL,
  `metric_nm` varchar(50) NOT NULL,
  `metric_val` bigint(20) NOT NULL,
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`profile_id`,`dt`,`metric_nm`),
  KEY `idx_dt` (`dt`),
  KEY `idx_metric` (`metric_nm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stg_ds_metric_hh`
--

DROP TABLE IF EXISTS `stg_ds_metric_hh`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `stg_ds_metric_hh` (
  `profile_id` varchar(20) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(3) unsigned NOT NULL,
  `metric_nm` varchar(50) NOT NULL,
  `metric_val` bigint(20) NOT NULL,
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`profile_id`,`dt`,`hh`,`metric_nm`),
  KEY `idx_dt` (`dt`),
  KEY `idx_hh` (`hh`),
  KEY `idx_metric` (`metric_nm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stg_ds_metric_hh_wide`
--

DROP TABLE IF EXISTS `stg_ds_metric_hh_wide`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `stg_ds_metric_hh_wide` (
  `profile_id` varchar(20) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(3) unsigned NOT NULL,
  `visit` bigint(20) NOT NULL DEFAULT 0,
  `uv` bigint(20) NOT NULL DEFAULT 0,
  `pageview` bigint(20) NOT NULL DEFAULT 0,
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`profile_id`,`dt`,`hh`),
  KEY `idx_dt` (`dt`),
  KEY `idx_hh` (`hh`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stg_hit_common`
--

DROP TABLE IF EXISTS `stg_hit_common`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `stg_hit_common` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `src` enum('webserver','wc') NOT NULL,
  `dt` date NOT NULL,
  `ts` datetime NOT NULL,
  `ip` varchar(45) DEFAULT NULL,
  `method` varchar(10) DEFAULT NULL,
  `url_raw` longtext DEFAULT NULL,
  `url_full` longtext DEFAULT NULL,
  `url_norm` longtext DEFAULT NULL,
  `host` varchar(255) DEFAULT NULL,
  `path` varchar(2048) DEFAULT NULL,
  `query` text DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  `bytes` bigint(20) DEFAULT NULL,
  `latency_ms` int(11) DEFAULT NULL,
  `ref` longtext DEFAULT NULL,
  `ref_host` varchar(255) DEFAULT NULL,
  `ua` longtext DEFAULT NULL,
  `kv_raw` longtext DEFAULT NULL,
  `uid` varchar(128) DEFAULT NULL,
  `pcid` varchar(64) DEFAULT NULL,
  `sid` varchar(64) DEFAULT NULL,
  `device_type` varchar(20) DEFAULT NULL,
  `evt` varchar(20) DEFAULT NULL,
  `accept_lang` varchar(255) DEFAULT NULL,
  `cc` char(2) DEFAULT NULL,
  `page_type` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_src_dt` (`src`,`dt`),
  KEY `idx_dt_ts` (`dt`,`ts`),
  KEY `idx_host_dt` (`host`,`dt`),
  KEY `idx_status_dt` (`status`,`dt`),
  KEY `idx_uid_dt` (`uid`,`dt`),
  KEY `idx_pcid_dt` (`pcid`,`dt`),
  KEY `idx_ref_host_dt` (`ref_host`,`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stg_wc_log_hit`
--

DROP TABLE IF EXISTS `stg_wc_log_hit`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `stg_wc_log_hit` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `dt` date NOT NULL,
  `ts` datetime NOT NULL,
  `ip` varchar(45) NOT NULL,
  `method` varchar(10) NOT NULL,
  `url_raw` text NOT NULL,
  `url_full` text NOT NULL,
  `url_norm` text NOT NULL,
  `host` varchar(255) DEFAULT NULL,
  `path` varchar(2048) DEFAULT NULL,
  `query` text DEFAULT NULL,
  `status` int(11) NOT NULL,
  `bytes` bigint(20) DEFAULT NULL,
  `ref` text DEFAULT NULL,
  `ua` text DEFAULT NULL,
  `kv_raw` text DEFAULT NULL,
  `uid` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10456490 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stg_webserver_log_hit`
--

DROP TABLE IF EXISTS `stg_webserver_log_hit`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `stg_webserver_log_hit` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `dt` date NOT NULL,
  `ts` datetime NOT NULL,
  `ip` varchar(45) DEFAULT NULL,
  `method` varchar(10) DEFAULT NULL,
  `url_raw` longtext DEFAULT NULL,
  `url_full` longtext DEFAULT NULL,
  `url_norm` longtext DEFAULT NULL,
  `host` varchar(255) DEFAULT NULL,
  `path` varchar(2048) DEFAULT NULL,
  `query` text DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  `bytes` bigint(20) DEFAULT NULL,
  `latency_ms` int(11) DEFAULT NULL,
  `ref` longtext DEFAULT NULL,
  `ref_host` varchar(255) DEFAULT NULL,
  `ua` longtext DEFAULT NULL,
  `kv_raw` longtext DEFAULT NULL,
  `uid` varchar(128) DEFAULT NULL,
  `pcid` varchar(64) DEFAULT NULL,
  `sid` varchar(64) DEFAULT NULL,
  `device_type` varchar(20) DEFAULT NULL,
  `evt` varchar(20) DEFAULT NULL,
  `accept_lang` varchar(255) DEFAULT NULL,
  `cc` char(2) DEFAULT NULL,
  `page_type` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_dt_ts` (`dt`,`ts`),
  KEY `idx_host_dt` (`host`,`dt`),
  KEY `idx_status_dt` (`status`,`dt`),
  KEY `idx_ref_host_dt` (`ref_host`,`dt`),
  KEY `idx_uid_dt` (`uid`,`dt`),
  KEY `idx_pcid_dt` (`pcid`,`dt`)
) ENGINE=InnoDB AUTO_INCREMENT=2293726 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `validation_result`
--

DROP TABLE IF EXISTS `validation_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `validation_result` (
  `validation_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `validation_run_id` bigint(20) unsigned NOT NULL,
  `profile_id` varchar(50) NOT NULL,
  `dt` date NOT NULL,
  `hh` tinyint(3) unsigned DEFAULT NULL,
  `rule_name` varchar(100) NOT NULL,
  `rule_group` varchar(50) NOT NULL,
  `metric_name` varchar(100) DEFAULT NULL,
  `layer_left` varchar(30) DEFAULT NULL,
  `layer_right` varchar(30) DEFAULT NULL,
  `observed_value` decimal(20,6) DEFAULT NULL,
  `expected_value` decimal(20,6) DEFAULT NULL,
  `diff_value` decimal(20,6) DEFAULT NULL,
  `diff_ratio` decimal(20,6) DEFAULT NULL,
  `validation_status` varchar(20) NOT NULL,
  `severity` varchar(20) NOT NULL,
  `note` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`validation_id`),
  KEY `idx_profile_dt` (`profile_id`,`dt`),
  KEY `idx_rule` (`rule_name`),
  KEY `idx_status` (`validation_status`),
  KEY `idx_run` (`validation_run_id`)
) ENGINE=InnoDB AUTO_INCREMENT=39058 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `validation_rule_definition`
--

DROP TABLE IF EXISTS `validation_rule_definition`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `validation_rule_definition` (
  `rule_id` varchar(100) NOT NULL,
  `rule_name` varchar(200) NOT NULL,
  `rule_type` varchar(50) NOT NULL,
  `severity` varchar(20) NOT NULL,
  `expression_sql` text NOT NULL,
  `description` text DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT 1,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`rule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `validation_run`
--

DROP TABLE IF EXISTS `validation_run`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `validation_run` (
  `validation_run_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `profile_id` varchar(50) NOT NULL,
  `dt_from` date NOT NULL,
  `dt_to` date NOT NULL,
  `started_at` datetime NOT NULL,
  `finished_at` datetime DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'running',
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`validation_run_id`),
  KEY `idx_profile_dt` (`profile_id`,`dt_from`,`dt_to`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB AUTO_INCREMENT=139 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `validation_summary_day`
--

DROP TABLE IF EXISTS `validation_summary_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `validation_summary_day` (
  `profile_id` varchar(50) NOT NULL,
  `dt` date NOT NULL,
  `validation_run_id` bigint(20) unsigned NOT NULL,
  `total_rules` int(11) NOT NULL DEFAULT 0,
  `pass_count` int(11) NOT NULL DEFAULT 0,
  `warn_count` int(11) NOT NULL DEFAULT 0,
  `fail_count` int(11) NOT NULL DEFAULT 0,
  `highest_severity` varchar(20) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`profile_id`,`dt`,`validation_run_id`),
  KEY `idx_dt` (`dt`),
  KEY `idx_run` (`validation_run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-04-07 10:09:23
