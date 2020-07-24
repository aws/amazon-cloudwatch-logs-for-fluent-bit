# Changelog

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
