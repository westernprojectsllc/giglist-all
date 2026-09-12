"""Load a region's ``config.py`` by path instead of through sys.path.

Every region directory (``mn/``, ``tn/``) contains a module named
``config``, and the scrapers are normally run as ``cd mn && python
scraper.py``, where sys.path[0] is the region directory and a bare
``from config import ...`` resolves correctly.

That breaks the moment two regions are imported into one process.
``tests/test_scrapers.py`` puts both directories on sys.path and imports
both scrapers, so ``config`` resolves to whichever region comes first —
and once it is in sys.modules under the bare name ``config``, the second
region gets the first one's values with no error at all. mn/scraper.py
was importing tn/config.py this way; it only appeared to work because the
two configs happen to share most of their top-level names.

Loading each region's config from its own directory under a
region-qualified module name keeps them distinct no matter what order
they are imported in, and leaves the ``cd mn && python scraper.py``
invocation working unchanged.
"""

import importlib.util
import sys
from pathlib import Path


def load_region_config(region_file):
    """Import the ``config.py`` sitting next to ``region_file``.

    Pass ``__file__`` from a region's scraper.py or render.py. The module
    is cached under a name derived from its directory (``mn`` ->
    ``giglist.region_config.mn``) so repeat calls are cheap and two
    regions never collide in sys.modules.
    """
    region_dir = Path(region_file).resolve().parent
    module_name = f"{__name__}.{region_dir.name}"

    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached

    config_path = region_dir / "config.py"
    spec = importlib.util.spec_from_file_location(module_name, config_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load region config from {config_path}")

    module = importlib.util.module_from_spec(spec)
    # Registered before exec_module so a config that imports itself (or
    # is loaded re-entrantly) sees the partially-initialised module
    # rather than starting a second execution.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        del sys.modules[module_name]
        raise
    return module
