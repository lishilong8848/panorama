from __future__ import annotations

import os

from main import main


if __name__ == "__main__":
    os.environ["QJPT_FORCE_ROLE_MODE"] = "internal"
    main()
