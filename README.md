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
* `log_group_name`: The name of the CloudWatch Log Group that you want log records sent to.
* `log_stream_name`: The name of the CloudWatch Log Stream that you want log records sent to.
* `log_stream_prefix`: Prefix for the Log Stream name. The tag is appended to the prefix to construct the full log stream name. Not compatible with the `log_stream_name` option.  
* `log_key`: By default, the whole log record will be sent to CloudWatch. If you specify a key name with this option, then only the value of that key will be sent to CloudWatch. For example, if you are using the Fluentd Docker log driver, you can specify `log_key log` and only the log message will be sent to CloudWatch.
* `role_arn`: ARN of an IAM role to assume (for cross account access).
* `auto_create_group`: Automatically create the log group. Valid values are "true" or "false" (case insensitive). Defaults to false.
* `endpoint`: Specify a custom endpoint for the CloudWatch Logs API.

**Note**: The plugin will always create the log stream, if it does not exist.

### Permissions

This plugin requires the following permissions:
* CreateLogGroup (if `auto_create_group` is set to true)
* CreateLogStream
* DescribeLogStreams
* PutLogEvents

### Credentials

This plugin uses the AWS SDK Go, and uses its [default credential provider chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html). If you are using the plugin on Amazon EC2 or Amazon ECS, the plugin will use your EC2 instance role or ECS Task role permissions. The plugin can also retrieve credentials from a (shared credentials file)[https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html], or from the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` environment variables.

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

### Fluent Bit Image built with plugins

We distribute a container image with Fluent Bit and these plugins. There are image tags for `latest` and the version of Fluent Bit that is built in the image. The first release is Fluent Bit `1.2.0`.

##### Docker Hub

[amazon/aws-for-fluent-bit](https://hub.docker.com/r/amazon/aws-for-fluent-bit/tags)

##### Amazon ECR

We also provide images in Amazon ECR in each AWS Region for high availability.

| Region         | Registry ID  | Full Image URI                                                          |
|----------------|--------------|-------------------------------------------------------------------------|
| us-east-1      | 906394416424 | 906394416424.dkr.ecr.us-east-1.amazonaws.com/aws-for-fluent-bit:latest      |
| eu-west-1      | 906394416424 | 906394416424.dkr.ecr.eu-west-1.amazonaws.com/aws-for-fluent-bit:latest      |
| us-west-1      | 906394416424 | 906394416424.dkr.ecr.us-west-1.amazonaws.com/aws-for-fluent-bit:latest      |
| ap-southeast-1 | 906394416424 | 906394416424.dkr.ecr.ap-southeast-1.amazonaws.com/aws-for-fluent-bit:latest |
| ap-northeast-1 | 906394416424 | 906394416424.dkr.ecr.ap-northeast-1.amazonaws.com/aws-for-fluent-bit:latest |
| us-west-2      | 906394416424 | 906394416424.dkr.ecr.us-west-2.amazonaws.com/aws-for-fluent-bit:latest      |
| sa-east-1      | 906394416424 | 906394416424.dkr.ecr.sa-east-1.amazonaws.com/aws-for-fluent-bit:latest      |
| ap-southeast-2 | 906394416424 | 906394416424.dkr.ecr.ap-southeast-2.amazonaws.com/aws-for-fluent-bit:latest |
| eu-central-1   | 906394416424 | 906394416424.dkr.ecr.eu-central-1.amazonaws.com/aws-for-fluent-bit:latest   |
| ap-northeast-2 | 906394416424 | 906394416424.dkr.ecr.ap-northeast-2.amazonaws.com/aws-for-fluent-bit:latest |
| ap-south-1     | 906394416424 | 906394416424.dkr.ecr.ap-south-1.amazonaws.com/aws-for-fluent-bit:latest     |
| us-east-2      | 906394416424 | 906394416424.dkr.ecr.us-east-2.amazonaws.com/aws-for-fluent-bit:latest      |
| ca-central-1   | 906394416424 | 906394416424.dkr.ecr.ca-central-1.amazonaws.com/aws-for-fluent-bit:latest   |
| eu-west-2      | 906394416424 | 906394416424.dkr.ecr.eu-west-2.amazonaws.com/aws-for-fluent-bit:latest      |
| eu-west-3      | 906394416424 | 906394416424.dkr.ecr.eu-west-3.amazonaws.com/aws-for-fluent-bit:latest      |
| ap-northeast-3 | 906394416424 | 906394416424.dkr.ecr.ap-northeast-3.amazonaws.com/aws-for-fluent-bit:latest |
| eu-north-1     | 906394416424 | 906394416424.dkr.ecr.eu-north-1.amazonaws.com/aws-for-fluent-bit:latest     |
| ap-east-1      | 449074385750 | 449074385750.dkr.ecr.ap-east-1.amazonaws.com/aws-for-fluent-bit:latest      |
| cn-north-1     | 128054284489 | 128054284489.dkr.ecr.cn-north-1.amazonaws.com/aws-for-fluent-bit:latest     |
| cn-northwest-1 | 128054284489 | 128054284489.dkr.ecr.cn-northwest-1.amazonaws.com/aws-for-fluent-bit:latest |
| us-gov-east-1  | 161423150738 | 161423150738.dkr.ecr.us-gov-east-1.amazonaws.com/aws-for-fluent-bit:latest  |
| us-gov-west-1  | 161423150738 | 161423150738.dkr.ecr.us-gov-west-1.amazonaws.com/aws-for-fluent-bit:latest  |

## License

This library is licensed under the Apache 2.0 License.
