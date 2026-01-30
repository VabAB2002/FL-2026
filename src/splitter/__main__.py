"""Allow running as: python -m src.splitter"""

import sys

from .cli import main

sys.exit(main())
