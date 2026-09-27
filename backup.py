# Maintainer: GABIT Marco Gantenbein
# Licence: GPLv3

"""odoo-backup entry point (the Docker image runs ``python backup.py``).

Usage: ``python backup.py [--check | --once [--database-only] | --retention-plan | --health]``;
``--help`` lists the options. All logic lives in the :mod:`odoo_backup` package. Importing this
module has no side effects: nothing is scheduled, no directory is cleaned.
"""

import sys

from odoo_backup.cli import main

if __name__ == "__main__":
    sys.exit(main())
