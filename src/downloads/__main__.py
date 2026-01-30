"""Allow running as: python -m src.downloads"""

import sys

from .cli import main

sys.exit(main())
