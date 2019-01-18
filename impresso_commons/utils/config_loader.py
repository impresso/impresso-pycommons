#!/usr/bin/env python36
# coding: utf-8 

"""
A class to load configuration files in json format, handling different task setting.
"""
__author__ = "maudehrmann"

import logging

from impresso_commons.utils.utils import parse_json

logger = logging.getLogger(__name__)


class Base:
    """ Base class for initial loading and default checking methods"""
    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_json(cls, json_file):
        logger.info(f"Loading config file {json_file}")
        config_dict = parse_json(json_file)
        return cls(config_dict)

    def check_params(self, config_dict, keys):
        if all(k in config_dict for k in keys):
            return True
        else:
            raise Exception(f"{self.__class__.__name__} misses a parameter among {keys}")

    def check_bucket(self, string, attribute):
        assert string in attribute, f"Bucket name should contain '{string}'"


class TextImporterConfig(Base):
    def __init__(self, config_dict):
        k = ['solr_server', 'solr_core', 's3_bucket_rebuilt', 'key_batches', 'newspapers']
        self.check_params(config_dict, k)
        self.solr_server = config_dict["solr_server"]
        self.solr_core = config_dict["solr_core"]
        self.bucket_rebuilt = config_dict["s3_bucket_rebuilt"]
        self.key_batches = config_dict["key_batches"]
        self.newspapers = config_dict["newspapers"]
        self.check_bucket("rebuilt", self.bucket_rebuilt)


class MentionBuilderConfig(Base):
    def __init__(self, config_dict):
        k = ['solr_server', 'solr_core_mentions', 's3_bucket_processed', 'newspapers']
        self.check_params(config_dict, k)

        self.solr_server = config_dict["solr_server"]
        self.solr_core_mentions = config_dict["solr_core_mentions"]
        self.bucket_processed = config_dict["s3_bucket_processed"]
        self.newspapers = config_dict["newspapers"]
        self.check_bucket("processed", self.bucket_processed)


class MentionImporterConfig(Base):
    def __init__(self, config_dict):
        k = ['solr_server', 'solr_core', 's3_bucket_processed', 'newspapers']
        self.check_params(config_dict, k)

        self.solr_server = config_dict["solr_server"]
        self.solr_core = config_dict["solr_core"]
        self.bucket_processed = config_dict["s3_bucket_processed"]
        self.newspapers = config_dict["newspapers"]
        self.check_bucket("processed", self.bucket_processed)


class TopicBuilderConfig(Base):
    def __init__(self, config_dict):
        k = ['solr_server', 'solr_core_topics', 's3_bucket_processed', 'newspapers', "process_type", "process_id"]
        self.check_params(config_dict, k)

        self.solr_server = config_dict["solr_server"]
        self.solr_core_mentions = config_dict["solr_core_topics"]
        self.bucket_processed = config_dict["s3_bucket_processed"]
        self.process_type = config_dict["process_type"]
        self.process_id = config_dict["process_id"]
        self.newspapers = config_dict["newspapers"]
        self.check_bucket("processed", self.bucket_processed)


class TopicImporterConfig(Base):
    def __init__(self, config_dict):
        k = ['solr_server', 'solr_core', 's3_bucket_processed', "newspapers"]
        self.check_params(config_dict, k)

        self.solr_server = config_dict["solr_server"]
        self.solr_core = config_dict["solr_core"]
        self.bucket_processed = config_dict["s3_bucket_processed"]
        self.newspapers = config_dict["newspapers"]
        self.check_bucket("processed", self.bucket_processed)


def main():
    file = "../config/solr_ci_builder_config.example.json"
    config = TextImporterConfig.from_json(file)
    print(config.newspapers)


if __name__ == '__main__':
    main()
