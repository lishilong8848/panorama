from __future__ import annotations

from app.bootstrap.app_factory import create_app


APP = create_app()


def get_app():
    return APP
