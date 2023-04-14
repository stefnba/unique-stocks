from shared.clients.db.postgres.client import db_client


class SurrogateKeyJobs:
    @staticmethod
    def add():
        return 1

    @staticmethod
    def get():
        return db_client.find()
        return 1
