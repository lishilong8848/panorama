from __future__ import annotations

import os

from app.config.runtime_role import FORCE_ROLE_MODE_ENV
from main import main


if __name__ == "__main__":
    os.environ[FORCE_ROLE_MODE_ENV] = "internal"
    main()
