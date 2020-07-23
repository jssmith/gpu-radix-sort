module github.com/nathantp/gpu-radix-sort/benchmark

go 1.13

require (
	github.com/go-delve/delve v1.4.1 // indirect
	github.com/pkg/errors v0.8.1
	github.com/serverlessresearch/srk v0.0.0-20200321035902-cc2031c5a52b
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.4.0
	golang.org/x/perf v0.0.0-20200318175901-9c9101da8316
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	gonum.org/v1/gonum v0.7.0
)

replace github.com/serverlessresearch/srk => ../../srk
