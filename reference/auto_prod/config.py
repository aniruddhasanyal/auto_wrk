class Config:
    def __init__(self):
        self.db_config = 'DRIVER={FreeTDS};SERVER=SDAQWGDWM4;DATABASE=GWCC_ProdCopy;UID=ClaimAnalyticsETLUser;PWD=jU' \
                         '&ruwrUde4aSpAt;TDS_Version=8.0;Port=1433 '

        self.input_folder = 'data/Production/Output'
        self.output_folder = 'data/Production/Input'
        self.LDAModel_path = 'lda_model/lda_trained.p'
