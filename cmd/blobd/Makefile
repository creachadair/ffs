# These settings control the image built by "make build".
DATAVOL ?= blobdata
NETWORK ?= 127.0.0.1
CNAME   ?= blobd
STORES  ?= badger,gcs,s3

# These settings control the container started by "make start".
STORE   ?= badger:blobdata.db
PORT    ?= 19741

.PHONY: build start stop

# Build an image containing the blobd binary.
build:
	docker build -t ffs/blobd --build-arg TAGS=$(STORES) ./docker

# Start a container running blobd with the given settings.
# The service is exported to the host under the same port.
start:
	docker run --detach --volume $(DATAVOL):/data \
		--name $(CNAME) \
		--publish $(NETWORK):$(PORT):$(PORT) \
		ffs/blobd \
		-store $(STORE) -listen :$(PORT)

stop:
	docker stop $(CNAME) && docker rm $(CNAME)
