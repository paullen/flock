module migrate

require (
	github.com/AndreasBriese/bbloom v0.0.0-20180913140656-343706a395b7 // indirect
	github.com/DATA-DOG/go-sqlmock v1.3.3
	github.com/GoogleCloudPlatform/cloudsql-proxy v0.0.0-20180921012930-634f0881b90b
	github.com/denisenkom/go-mssqldb v0.0.0-20180901172138-1eb28afdf9b6
	github.com/dgraph-io/badger v1.5.4
	github.com/dgryski/go-farm v0.0.0-20180109070241-2de33835d102 // indirect
	github.com/elgris/sqrl v0.0.0-20180926210025-d09d22f2d5ac
	github.com/google/go-cmp v0.2.0 // indirect
	github.com/google/uuid v1.0.0
	github.com/lib/pq v1.0.0
	github.com/magefile/mage v1.8.0
	github.com/pkg/errors v0.8.0 // indirect
	github.com/srikrsna/flock v0.0.0-20180927140816-10ea336ca833
	golang.org/x/crypto v0.0.0-20180910181607-0e37d006457b // indirect
	google.golang.org/api v0.0.0-20180927231558-81028c6d7fe8 // indirect
	google.golang.org/grpc v1.15.0
)

replace github.com/srikrsna/flock => ../../flock-master/flock-master
