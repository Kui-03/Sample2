from classes.predictor_models import *
from classes.user_models import User
from services.user_service import UserService

# user = User("2198-3392916-2")
# serv = UserService()
# serv.getUser(user.ssn)
# print(serv.user.__dict__)

# x = serv.user.format()
# print(pd.read_json(x))

# p = Predictor("XGB")
# y_predict = p.predict(pd.read_json(x))

# print(y_predict)
# print(serv.listUsers())

from pages.main import *
if __name__ == "__main__":
    app.run(port=PORT, debug=True)