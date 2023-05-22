# Changelog

## 1.9.4
* Bug - Fix utf-8 calculation of payload length to account for invalid unicode bytes that will be replaced with the 3 byte unicode replacement character. This bug can lead to an `InvalidParameterException` from CloudWatch when the payload sent is calculated to be over the limit due to character replacement.

## 1.9.3
* Enhancement - Upgrade Go version to 1.20

## 1.9.2
* Bug - Fixed Log Loss can occur when log group creation or retention policy API calls fail. (#314)

## 1.9.1
* Enhancement - Added different base user agent for Linux and Windows

## 1.9.0
* Feature - Add support for building this plugin on Windows. *Note that this is only support in this plugin repo for Windows compilation.*

## 1.8.0
* Feature - Add `auto_create_stream ` option (#257)
* Bug - Allow recovery from a stream being deleted and created by a user (#257)

## 1.7.0
* Feature - Add support for external_id (#226)

## 1.6.4
* Bug - Remove corrupted unicode fragments on truncation (#208)

## 1.6.3
* Enhancement - Upgrade Go version to 1.17

## 1.6.2
* Enhancement - Add validation to stop accepting both of `log_stream_name` and `log_stream_prefix` together (#190)

## 1.6.1
* Enhancement - Delete debug messages which make log info useless (#146)

## 1.6.0
* Enhancement - Add support for updating the retention policy of existing log groups (#121)

## 1.5.0
* Feature - Automatically re-create CloudWatch log groups and log streams if they are deleted (#95)
* Feature - Add default fallback log group and stream names (#99)
* Feature - Add support for ECS Metadata and UUID via special variables in log stream and group names (#108)
* Enhancement - Remove invalid characters in log stream and log group names (#103)

## 1.4.1
* Bug - Add back `auto_create_group` option (#96)
* Bug - Truncate log events to max size (#85)

## 1.4.0
* Feature - Add support for dynamic log group names (#46)
* Feature - Add support for dynamic log stream names (#16)
* Feature - Support tagging of newly created log groups (#51)
* Feature - Support setting log group retention policies (#50)

## 1.3.1
* Bug - Check for empty logEvents before calling PutLogEvents (#66)

## 1.3.0
* Feature - Add sts_endpoint param for custom STS API endpoint (#55)

## 1.2.0
* Feature - Add support for Embedded Metric Format (#27)

## 1.1.1
* Bug - Discard and do not send empty messages (#40)

## 1.1.0
* Bug - A single CloudWatch Logs PutLogEvents request can not contain logs that span more than 24 hours (#29)
* Feature - Add `credentials_endpoint` option (#36)
* Feature - Support IAM Roles for Service Accounts in Amazon EKS (#33)

## 1.0.0
Initial versioned release of the Amazon CloudWatch Logs for Fluent Bit Plugin
