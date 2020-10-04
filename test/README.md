# CloudWatch Logs Test

The application in this folder provides a way to test the performance of the
cloudwatch logs Go plugin. It currently just does a pprof reading, but can be
quickly adapted to do more or other tests.

## pprof

Here's some quick-start info on how to use this.

```shell
go run .
go tool pprof cloudwatchlogs.prof
```

The above commands create a profile file and drop you into a pprof shell.
From that shell, you can run a few commands which I learned from
[this blog post](https://blog.golang.org/pprof).

Here are all the commands from the linked blog post:

```shell
top5
top5 -cum
top10
web
web mapaccess1
list DFS
list FindLoops
web mallocgc
```

The `web` commands opens an SVG file in a web browser. You can save the file
from your web browser and share it with others. It's often a nice visual.

1.  At the time of this writing, JSON marshaling is the slowest part of this code.
1.  The second slowest thing is parsing log values into stream and group names.
