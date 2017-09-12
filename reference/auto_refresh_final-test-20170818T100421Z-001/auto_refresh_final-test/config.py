class Config:
    def __init__(self):
        self.in_db_config = {'DRIVER': '{FreeTDS}',
                             'SERVER': 'SDAQWGDWM4',
                             'DATABASE': 'GWCC_ProdCopy',
                             'UID': 'ClaimAnalyticsETLUser',
                             'PWD': 'jU&ruwrUde4aSpAt',
                             'TDS_Version': '8.0',
                             'Port': '1433'}

        self.out_db_config = {'DRIVER': '{FreeTDS}',
                              'SERVER': 'SQLNAD-CommUW',
                              'DATABASE': 'CommUW3',
                              'UID': 'CommUWAdmin',
                              'PWD': '5huWR?',
                              'TDS_Version': '8.0',
                              'Port': '1433'}

        self.input_folder = 'data/production/input'
        self.output_folder = 'data/production/output'
        self.model_path = 'data/production/models'

    def get_db_conn(self, out_db=False):
        db_details = self.in_db_config
        if out_db: db_details = self.out_db_config
        return ';'.join('{}={}'.format(k, v) for k, v in db_details.items())
