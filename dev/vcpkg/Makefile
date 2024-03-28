DOCKER_IMAGE=gluten-builder-vcpkg
GLUTEN_REPO=$(shell realpath -L ../..)

CCACHE_DIR=$(HOME)/.ccache
VCPKG_BINARY_CACHE_DIR=$(HOME)/.cache/vcpkg
MAVEN_M2_DIR=$(HOME)/.m2

$(info $(GLUTEN_REPO))

.PHONY: docker-image build
build: docker-image | $(CCACHE_DIR) $(VCPKG_BINARY_CACHE_DIR) $(MAVEN_M2_DIR)
	docker run --rm -ti \
		-v $(GLUTEN_REPO):$(GLUTEN_REPO) \
		-v $(VCPKG_BINARY_CACHE_DIR):/home/build/.cache/vcpkg \
		-v $(MAVEN_M2_DIR):/home/build/.m2 \
		-v $(CCACHE_DIR):/home/build/.ccache \
		--workdir $(GLUTEN_REPO) \
		-ti \
		$(DOCKER_IMAGE) \
		./dev/package-vcpkg.sh

docker-image:
	docker build \
		--file docker/Dockerfile \
		--build-arg BUILDER_UID=`id -u` \
		--build-arg BUILDER_GID=`id -g` \
		--tag "$(DOCKER_IMAGE)" \
		.

docker-image-gha:
	docker build \
		--file docker/Dockerfile.gha \
		--tag "$(DOCKER_IMAGE)" \
		.


$(CCACHE_DIR) $(VCPKG_BINARY_CACHE_DIR) $(MAVEN_M2_DIR): %:
	mkdir -p $@
