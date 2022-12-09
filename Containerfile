# SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
# SPDX-License-Identifier: MIT
#
# Containerfile for running the build-sitelinks tool in production.
#
# As of December 2022, the Wikimedia cloud does not support custom containers.
# Therefore, we use this container merely to build our release binary
# on the same platform as gets used in production. We then copy the
# binary out of the image, and install it on the Wikimedia cloud with scp.
# Such manual shuffling is the opposite of good engineering practice,
# but it really seems to be the recommended way of deployment of Toolforge.
# So, here’s our current "release process". If anyone wants to volunteer
# at Wikimedia to make this more reasonable, a huge thank-you in advance.
#
# $ podman build -t sitelinks .
# $ podman run -it sitelinks
# $ podman ps    # note down the running container, eg. "quizzical_mcclintock"
# $ podman cp quizzical_mcclintock:/usr/local/bin/build-sitelinks .
# $ scp build-sitelinks login.toolforge.org:build-sitelinks
# $ ssh login.toolforge.org
# $ chgrp tools.sitelinks build-sitelinks
# $ mv build-sitelinks /data/project/sitelinks/build-sitelinks
# $ become sitelinks
# $ take build-sitelinks

FROM rust:slim-bullseye AS builder
COPY . /build
WORKDIR /build
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /build/target/release/build-sitelinks \
    /usr/local/bin/build-sitelinks
