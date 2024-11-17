# @Author  : yuanzi
# @Time    : 2024/11/17 10:12
# Website: https://www.yzgsa.com
# Copyright (c) <yuanzigsa@gmail.com>

from nettraffic_analyzer.es import Es
from nettraffic_analyzer.utils import *


if __name__ == "__main__":
    logger = setup_logger()
    logger.info(banner)

    es = Es()
    es.run()
