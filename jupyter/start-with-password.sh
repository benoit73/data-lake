#!/bin/bash
# Démarre Jupyter avec un mot de passe (hashé au démarrage depuis $JUPYTER_PASSWORD) au lieu d'un token.
HASH=$(python -c "import os; from jupyter_server.auth import passwd; print(passwd(os.environ['JUPYTER_PASSWORD']))")
exec start-notebook.py --PasswordIdentityProvider.hashed_password="$HASH" --IdentityProvider.token='' "$@"
