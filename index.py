from src.app.tableBackUp import CodeBookBackUp
from src.config.appConfig import getJsonConfig, initConfigs

initConfigs()
jsonConfig = getJsonConfig()

username = jsonConfig['db_username']
password = jsonConfig['db_password']
orclDbConnStr = jsonConfig['appDbConnStr']
table_name = jsonConfig['table_name']
output_dir = jsonConfig['output_dir']

codeBookBackUpRepo = CodeBookBackUp(orclDbConnStr)
isInsSuccess = codeBookBackUpRepo.codeBookTableBackUp(table_name, output_dir)

