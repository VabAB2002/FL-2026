"""Allow running as: python -m src.readers"""

import sys

from .cli import main

sys.exit(main())
