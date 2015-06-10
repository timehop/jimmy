# jimmy

Higher-level wrapper for [Redigo](http://github.com/garyburd/redigo).

## Dependencies

Just [Redigo](http://github.com/garyburd/redigo).

## Testing

Testing requires [Ginkgo](http://github.com/onsi/ginkgo)
and [Gomega](http://github.com/onsi/gomega): `go get github.com/onsi/ginkgo github.com/onsi/gomega`.

You’ll need Ginkgo installed so you can run the `ginkgo` tool: `go install github.com/onsi/ginkgo`.
(And make sure `$GOPATH/bin` is in your `$PATH`.)

You’ll also need Redis running locally and accessible at `localhost:6379`.

**Warning:** running the tests will **ERASE** all keys in database 10 in your local Redis.

To run the tests just run `ginkgo`.
