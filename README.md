[![Test Actions Status](https://github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/workflows/Build/badge.svg)](https://github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/actions)

## Fluent Bit Plugin for CloudWatch Logs

A Fluent Bit output plugin for CloudWatch Logs

#### Security disclosures

If you think you’ve found a potential security issue, please do not post it in the Issues.  Instead, please follow the instructions [here](https://aws.amazon.com/security/vulnerability-reporting/) or email AWS security directly at [aws-security@amazon.com](mailto:aws-security@amazon.com).

### Usage

Run `make` to build `./bin/cloudwatch.so`. Then use with Fluent Bit:
```
./fluent-bit -e ./cloudwatch.so -i cpu \
-o cloudwatch \
-p "region=us-west-2" \
-p "log_group_name=fluent-bit-cloudwatch" \
-p "log_stream_name=testing" \
-p "auto_create_group=true"
```

### Plugin Options

* `region`: The AWS region.
* `log_group_name`: The name of the CloudWatch Log Group that you want log records sent to. This value allows a template in the form of `$(variable)`. See `log_stream_name` description for more. The app will attempt to create missing log groups, and will throw an error if it does not have access.
* `log_stream_name`: The name of the CloudWatch Log Stream that you want log records sent to. This value allows a template in the form of `$(variable)` where
* `default_log_group_name`: This required variable is the fallback in case any variables in `log_group_name` fails to parse. Defaults to `fluentbit-default`
* `default_log_stream_name`: This required variable is the fallback in case any variables in `log_stream_name` fails to parse. Defaults to `/fluentbit-default`
`variable` is a map key name in the log message. To access sub-values in the map
use the form `$(variable['subkey'])`. Special values: `$(tag)` references the full tag name, `$(tag[0])` and `$(tag[1])` are the first and second values of log tag split on periods. You may access any member by index, 0 through 9.
* `log_stream_prefix`: (deprecated) Prefix for the Log Stream name. Setting this to `prefix-` is the same as setting `log_stream_name = prefix-$(tag)`.
* `log_key`: By default, the whole log record will be sent to CloudWatch. If you specify a key name with this option, then only the value of that key will be sent to CloudWatch. For example, if you are using the Fluentd Docker log driver, you can specify `log_key log` and only the log message will be sent to CloudWatch.
* `log_format`: An optional parameter that can be used to tell CloudWatch the format of the data. A value of `json/emf` enables CloudWatch to extract custom metrics embedded in a JSON payload. See the [Embedded Metric Format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html).
* `role_arn`: ARN of an IAM role to assume (for cross account access).
* `auto_create_group`: Automatically create log groups (and add tags). Valid values are "true" or "false" (case insensitive). Defaults to false. If you use dynamic variables in your log group name, you may need this to be `true`.
* `new_log_group_tags`: Comma/equal delimited string of tags to include with _auto created_ log groups. Example: `"tag=val,cooltag2=my other value"`
* `log_retention_days`: If set to a number greater than zero, and newly create log group's retention policy is set to this many days.
* `endpoint`: Specify a custom endpoint for the CloudWatch Logs API.
* `sts_endpoint`: Specify a custom endpoint for the STS API; used to assume your custom role provided with `role_arn`.
* `credentials_endpoint`: Specify a custom HTTP endpoint to pull credentials from. The HTTP response body should look like the following:
```
{
    "AccessKeyId": "ACCESS_KEY_ID",
    "Expiration": "EXPIRATION_DATE",
    "SecretAccessKey": "SECRET_ACCESS_KEY",
    "Token": "SECURITY_TOKEN_STRING"
}
```

**Note**: The plugin will always create the log stream, if it does not exist.

### Permissions

This plugin requires the following permissions:
* CreateLogGroup (useful when using dynamic groups)
* CreateLogStream
* DescribeLogStreams
* PutLogEvents
* PutRetentionPolicy (if `log_retention_days` is set > 0)

### Credentials

This plugin uses the AWS SDK Go, and uses its [default credential provider chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html). If you are using the plugin on Amazon EC2 or Amazon ECS or Amazon EKS, the plugin will use your EC2 instance role or [ECS Task role permissions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html) or [EKS IAM Roles for Service Accounts for pods](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html). The plugin can also retrieve credentials from a [shared credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html), or from the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` environment variables.

### Environment Variables

* `FLB_LOG_LEVEL`: Set the log level for the plugin. Valid values are: `debug`, `info`, and `error` (case insensitive). Default is `info`. **Note**: Setting log level in the Fluent Bit Configuration file using the Service key will not affect the plugin log level (because the plugin is external).
* `SEND_FAILURE_TIMEOUT`: Allows you to configure a timeout if the plugin can not send logs to CloudWatch. The timeout is specified as a [Golang duration](https://golang.org/pkg/time/#ParseDuration), for example: `5m30s`. If the plugin has failed to make any progress for the given period of time, then it will exit and kill Fluent Bit. This is useful in scenarios where you want your logging solution to fail fast if it has been misconfigured (i.e. network or credentials have not been set up to allow it to send to CloudWatch).

### Fluent Bit Versions

This plugin has been tested with Fluent Bit 1.2.0+. It may not work with older Fluent Bit versions. We recommend using the latest version of Fluent Bit as it will contain the newest features and bug fixes.

### Example Fluent Bit Config File

```
[INPUT]
    Name        forward
    Listen      0.0.0.0
    Port        24224

[OUTPUT]
    Name cloudwatch
    Match   *
    region us-east-1
    log_group_name fluent-bit-cloudwatch
    log_stream_prefix from-fluent-bit-
    auto_create_group true
```

### AWS for Fluent Bit

We distribute a container image with Fluent Bit and these plugins.

##### GitHub

[github.com/aws/aws-for-fluent-bit](https://github.com/aws/aws-for-fluent-bit)

##### Docker Hub

[amazon/aws-for-fluent-bit](https://hub.docker.com/r/amazon/aws-for-fluent-bit/tags)

##### Amazon ECR

You can use our SSM Public Parameters to find the Amazon ECR image URI in your region:

```
aws ssm get-parameters-by-path --path /aws/service/aws-for-fluent-bit/
```

For more see [our docs](https://github.com/aws/aws-for-fluent-bit#public-images).

## License

This library is licensed under the Apache 2.0 License.
