module github.com/ghetzel/pivot/v3

require (
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78 // indirect
	github.com/Microsoft/go-winio v0.4.11 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/RoaringBitmap/roaring v0.4.4 // indirect
	github.com/Smerity/govarint v0.0.0-20150407073650-7265e41f48f1 // indirect
	github.com/alexcesaro/statsd v2.0.0+incompatible
	github.com/aws/aws-sdk-go v1.13.26
	github.com/blevesearch/bleve v0.7.0
	github.com/blevesearch/blevex v0.0.0-20180227211930-4b158bb555a3 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.2 // indirect
	github.com/blevesearch/segment v0.0.0-20160915185041-762005e7a34f // indirect
	github.com/boltdb/bolt v0.0.0-20171120010307-9da317453632 // indirect
	github.com/containerd/continuity v0.0.0-20180919190352-508d86ade3c2 // indirect
	github.com/couchbase/vellum v0.0.0-20180314210611-5083a469fcef // indirect
	github.com/cznic/b v0.0.0-20180115125044-35e9bbe41f07 // indirect
	github.com/cznic/mathutil v0.0.0-20181021201202-eba54fb065b7 // indirect
	github.com/cznic/strutil v0.0.0-20171016134553-529a34b1c186 // indirect
	github.com/deckarep/golang-set v0.0.0-20171013212420-1d4478f51bed
	github.com/docker/go-connections v0.3.0 // indirect
	github.com/docker/go-units v0.3.3 // indirect
	github.com/edsrzf/mmap-go v0.0.0-20170320065105-0bce6a688712 // indirect
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/fatih/structs v1.0.0
	github.com/ghetzel/cli v1.17.0
	github.com/ghetzel/diecast v1.17.34
	github.com/ghetzel/go-stockutil v1.8.81
	github.com/ghodss/yaml v1.0.0
	github.com/glycerine/go-unsnap-stream v0.0.0-20180323001048-9f0cb55181dd // indirect
	github.com/glycerine/goconvey v0.0.0-20180728074245-46e3a41ad493 // indirect
	github.com/go-ini/ini v1.34.0 // indirect
	github.com/go-sql-driver/mysql v1.3.0
	github.com/golang/snappy v0.0.0-20160407051505-cef980a12b31 // indirect
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/gotestyourself/gotestyourself v2.1.0+incompatible // indirect
	github.com/hashicorp/golang-lru v0.5.1
	github.com/husobee/vestigo v1.1.0
	github.com/jbenet/go-base58 v0.0.0-20150317085156-6237cf65f3a6
	github.com/jdxcode/netrc v0.0.0-20180207092346-e1a19c977509
	github.com/jmespath/go-jmespath v0.0.0-20160202185014-0b12d6b521d8 // indirect
	github.com/jmhodges/levigo v0.0.0-20161115193449-c42d9e0ca023 // indirect
	github.com/jtolds/gls v4.2.1+incompatible // indirect
	github.com/lib/pq v1.1.0
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mattn/go-shellwords v1.0.10 // indirect
	github.com/mattn/go-sqlite3 v1.9.0
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/opencontainers/runc v1.0.0-rc5 // indirect
	github.com/orcaman/concurrent-map v0.0.0-20180319144342-a05df785d2dc
	github.com/ory/dockertest v3.3.2+incompatible
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20170806203942-52369c62f446 // indirect
	github.com/smartystreets/assertions v0.0.0-20180927180507-b2de0cb4f26d // indirect
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c // indirect
	github.com/steveyen/gtreap v0.0.0-20150807155958-0abe01ef9be2 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/syndtr/goleveldb v0.0.0-20181105012736-f9080354173f // indirect
	github.com/tecbot/gorocksdb v0.0.0-20181010114359-8752a9433481 // indirect
	github.com/tinylib/msgp v1.0.2 // indirect
	github.com/urfave/negroni v1.0.1-0.20191011213438-f4316798d5d3
	github.com/willf/bitset v0.0.0-20161202170036-5c3c0fce4884 // indirect
	golang.org/x/sys v0.0.0-20200523222454-059865788121 // indirect
	gopkg.in/alexcesaro/statsd.v2 v2.0.0 // indirect
	gopkg.in/ini.v1 v1.39.0 // indirect
	gopkg.in/mgo.v2 v2.0.0-20160818020120-3f83fa500528
	gotest.tools v2.1.0+incompatible // indirect
)

go 1.13

//  replace github.com/ghetzel/go-stockutil v1.8.62 => ../go-stockutil
