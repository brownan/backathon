class BackathonRouter:
    """Makes sure we don't apply backathon migrations to the default database

    Our models should always go into a database managed by the Repository
    class. This router stops the default migrate command from trying to
    create our tables in the default database.

    Later we may want to have some models in the default database and others
    in the repository-specific databases. That logic goes in here.
    """

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == 'default' and app_label == 'backathon':
            return False