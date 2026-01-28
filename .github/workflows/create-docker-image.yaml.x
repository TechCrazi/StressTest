name: Build Docker image (github-action tag)

on:
  workflow_dispatch:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: [self-hosted, docker]   # matches your myoung34 runners

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Show runner info
        run: |
          uname -a
          arch
          docker version

      - name: Build Docker image
        run: |
          IMAGE_NAME=stress-test
          SHORT_SHA=${GITHUB_SHA::7}

          echo "Building Docker images:"
          echo " - ${IMAGE_NAME}:github-action"
          echo " - ${IMAGE_NAME}:${SHORT_SHA}"

          docker build \
            -t ${IMAGE_NAME}:github-action \
            -t ${IMAGE_NAME}:${SHORT_SHA} \
            .

          docker images | grep ${IMAGE_NAME}