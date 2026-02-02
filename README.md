To start the venv

'''
venv/Scripts/activate
'''


To Start DOCKER

'''
docker compose --env-file configs/postgres/.env up -d
'''

Run Tests
"""
python -m unittest discover -s tests/unittest
"""