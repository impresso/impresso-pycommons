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


class PartitionerConfig(Base):
    def __init__(self, config_dict):
        k = ['s3_bucket_rebuilt', "newspapers", 'output_dir', 'local_fs', 'keep_full']
        self.check_params(config_dict, k)

        self.bucket_name = config_dict["s3_bucket_rebuilt"]
        self.newspapers = config_dict["newspapers"]
        self.output_dir = config_dict["output_dir"]
        self.local_fs = config_dict["local_fs"]
        self.keep_full = config_dict["keep_full"]
        self.check_bucket("rebuilt", self.bucket_name)


def main():
    file = "../config/solr_ci_builder_config.example.json"
    config = PartitionerConfig.from_json(file)
    print(config.newspapers)


if __name__ == '__main__':
    main()
