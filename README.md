# digdag-gcs-download

digdag gcp plugin download from  Google CloudStorage 

## How to use

### Create file
[test.dig]
```
timezone: Asia/Tokyo
_export:
  plugin:
    repositories:
      - https://dl.bintray.com/gymxxx/maven/
    dependencies:
      - io.digdag.plugin.digdag-gcs-download:0.1.0

+step0:
  bq_download>:
  bucket: test_bucket
  prefix: test_prefix
  out_file: ./test.out
```

### Run

```
$ digdag run test.dig
```
