[![Test Actions Status](https://github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/workflows/Build/badge.svg)](https://github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/actions)

## Fluent Bit Plugin for CloudWatch Logs

**NOTE: A new higher performance Fluent Bit CloudWatch Logs Plugin has been released.** Check out our [official guidance](#new-higher-performance-core-fluent-bit-plugin).

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

For building Windows binaries, we need to install `mingw-w64` for cross-compilation. The same can be done using-
```
sudo apt-get install -y gcc-multilib gcc-mingw-w64
```
After this step, run `make windows-release`. Then use with Fluent Bit on Windows:
```
./fluent-bit.exe -e ./cloudwatch.dll -i dummy `
-o cloudwatch `
-p "region=us-west-2" `
-p "log_group_name=fluent-bit-cloudwatch" `
-p "log_stream_name=testing" `
-p "auto_create_group=true"
```

### Plugin Options

* `region`: The AWS region.
* `log_group_name`: The name of the CloudWatch Log Group that you want log records sent to. This value allows a template in the form of `$(variable)`. See section [Templating Log Group and Stream Names](#templating-log-group-and-stream-names) for more. Fluent Bit will create missing log groups if `auto_create_group` is set, and will throw an error if it does not have permission.
* `log_stream_name`: The name of the CloudWatch Log Stream that you want log records sent to. This value allows a template in the form of `$(variable)`. See section [Templating Log Group and Stream Names](#templating-log-group-and-stream-names) for more.
* `default_log_group_name`: This required variable is the fallback in case any variables in `log_group_name` fails to parse. Defaults to `fluentbit-default`.
* `default_log_stream_name`: This required variable is the fallback in case any variables in `log_stream_name` fails to parse. Defaults to `/fluentbit-default`.
* `log_stream_prefix`: (deprecated) Prefix for the Log Stream name. Setting this to `prefix-` is the same as setting `log_stream_name = prefix-$(tag)`.
* `log_key`: By default, the whole log record will be sent to CloudWatch. If you specify a key name with this option, then only the value of that key will be sent to CloudWatch. For example, if you are using the Fluentd Docker log driver, you can specify `log_key log` and only the log message will be sent to CloudWatch.
* `log_format`: An optional parameter that can be used to tell CloudWatch the format of the data. A value of `json/emf` enables CloudWatch to extract custom metrics embedded in a JSON payload. See the [Embedded Metric Format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html).
* `role_arn`: ARN of an IAM role to assume (for cross account access).
* `auto_create_group`: Automatically create log groups (and add tags). Valid values are "true" or "false" (case insensitive). Defaults to false. If you use dynamic variables in your log group name, you may need this to be `true`.
* `auto_create_stream`: Automatically create log streams. Valid values are "true" or "false" (case insensitive). Defaults to true.
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

### Templating Log Group and Stream Names

 A template in the form of `$(variable)` can be set in `log_group_name` or `log_stream_name`. `variable` can be a map key name in the log message. To access sub-values in the map use the form `$(variable['subkey'])`. Also, it can be replaced with special values to insert the tag, ECS metadata or a random string in the name.

 Special Values:
 *  `$(tag)` references the full tag name, `$(tag[0])` and `$(tag[1])` are the first and second values of log tag split on periods. You may access any member by index, 0 through 9.
 *  `$(uuid)` will insert a random string in the names. The random string is generated automatically with format: 4 bytes of time (seconds) + 16 random bytes. It is created when the plugin starts up and uniquely identifies the output - which means that until Fluent Bit is restarted, it will be the same. If you have multiple CloudWatch outputs, each one will get a unique UUID.
 * If your container is running in ECS, `$(variable)` can be set as `$(ecs_task_id)`, `$(ecs_cluster)` or `$(ecs_task_arn)`. It will set ECS metadata into `log_group_name` or `log_stream_name`.

 Here is an example for `fluent-bit.conf`:

```
[INPUT]
    Name        dummy
    Tag         dummy.data
    Dummy {"pam": {"item": "soup", "item2":{"subitem": "rice"}}}

[OUTPUT]
    Name cloudwatch
    Match   *
    region us-east-1
    log_group_name fluent-bit-cloudwatch-$(uuid)-$(tag)
    log_stream_name from-fluent-bit-$(pam['item2']['subitem'])-$(ecs_task_id)-$(ecs_cluster)
    auto_create_group true
```

And here is the resulting log stream name and log group name:

```
log_group_name fluent-bit-cloudwatch-1jD7P6bbSRtbc9stkWjJZYerO6s-dummy.data
log_stream_name from-fluent-bit-rice-37e873f6-37b4-42a7-af47-eac7275c6152-ecs-local-cluster
```

#### Templating Log Group and Stream Names based on Kubernetes metadata

If you enable the kubernetes filter, then metadata like the following will be added to each log:

```
kubernetes: {
    annotations: {
        "kubernetes.io/psp": "eks.privileged"
    },
    container_hash: "<some hash>",
    container_name: "myapp",
    docker_id: "<some id>",
    host: "ip-10-1-128-166.us-east-2.compute.internal",
    labels: {
        app: "myapp",
        "pod-template-hash": "<some hash>"
    },
    namespace_name: "default",
    pod_id: "198f7dd2-2270-11ea-be47-0a5d932f5920",
    pod_name: "myapp-5468c5d4d7-n2swr"
}
```

For help setting up Fluent Bit with kubernetes please see [Kubernetes Logging Powered by AWS for Fluent Bit](https://aws.amazon.com/blogs/containers/kubernetes-logging-powered-by-aws-for-fluent-bit/) or [Set up Fluent Bit as a DaemonSet to send logs to CloudWatch Logs](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Container-Insights-setup-logs-FluentBit.html).

The kubernetes metadata can be referenced just like any other keys using the templating feature, for example, the following will result in a log group name which is `/eks/{namespace_name}/{pod_name}`. 

```
    [OUTPUT]
      Name              cloudwatch
      Match             kube.*
      region            us-east-1
      log_group_name    /eks/$(kubernetes['namespace_name'])/$(kubernetes['pod_name'])
      log_stream_name   $(kubernetes['namespace_name'])/$(kubernetes['container_name'])
      auto_create_group true
```

### New Higher Performance Core Fluent Bit Plugin

In the summer of 2020, we released a [new higher performance CloudWatch Logs plugin](https://docs.fluentbit.io/manual/pipeline/outputs/cloudwatch) named `cloudwatch_logs`.

That plugin has a core subset of the features of this older, lower performance and less efficient plugin. Check out its [documentation](https://docs.fluentbit.io/manual/pipeline/outputs/cloudwatch).

#### Do you plan to deprecate this older plugin?

At this time, we do not. This plugin will continue to be supported. It contains features that have not been ported to the higher performance version. Specifically, the feature for [templating of log group name and streams with ECS Metadata or values in the logs](#templating-log-group-and-stream-names). While [simple templating support](https://docs.fluentbit.io/manual/pipeline/outputs/cloudwatch#log-stream-and-group-name-templating-using-record_accessor-syntax) now exists in the high performance plugin, it does not have all of the features of the plugin in this repo. Some users will continue to need the features in this repo. 

#### Which plugin should I use?

If the features of the higher performance plugin are sufficient for your use cases, please use it. It can achieve higher throughput and will consume less CPU and memory.

#### How can I migrate to the higher performance plugin?

It supports a subset of the options of this plugin. For many users, you can simply replace the plugin name `cloudwatch` with the new name `cloudwatch_logs`. Check out its [documentation](https://docs.fluentbit.io/manual/pipeline/outputs/cloudwatch). 

#### Do you accept contributions to both plugins?

Yes. The high performance plugin is written in C, and this plugin is written in Golang. We understand that Go is an easier language for amateur contributors to write code in- that is a key reason why we are continuing to maintain it.

However, if you can write code in C, please consider contributing new features to the [higher performance plugin](https://github.com/fluent/fluent-bit/tree/master/plugins/out_cloudwatch_logs).

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

##### Amazon ECR Public Gallery

[aws-for-fluent-bit](https://gallery.ecr.aws/aws-observability/aws-for-fluent-bit)

Our images are available in Amazon ECR Public Gallery. You can download images with different tags by following command:

```
docker pull public.ecr.aws/aws-observability/aws-for-fluent-bit:<tag>
```

For example, you can pull the image with latest version by:

```
docker pull public.ecr.aws/aws-observability/aws-for-fluent-bit:latest
```

If you see errors for image pull limits, try log into public ECR with your AWS credentials:

```
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
```

You can check the [Amazon ECR Public official doc](https://docs.aws.amazon.com/AmazonECR/latest/public/get-set-up-for-amazon-ecr.html) for more details.

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
