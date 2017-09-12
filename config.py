class Config:
    def __init__(self):
        self.db_config = {'DRIVER': '{FreeTDS}',
                          'SERVER': 'SDAQWGDWM4',
                          'DATABASE': 'GWCC_ProdCopy',
                          'UID': 'ClaimAnalyticsETLUser',
                          'PWD': 'jU&ruwrUde4aSpAt',
                          'TDS_Version': '8.0',
                          'Port': '1433'}

        self.input_folder = 'data/production/input'
        self.output_folder = 'data/production/output'
        self.model_path = 'data/production/models'

    def get_db_conn(self):
        return ';'.join('{}={}'.format(k, v) for k, v in self.db_config.items())
