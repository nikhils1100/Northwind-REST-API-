from flask import Flask, request, jsonify, make_response
from flask_marshmallow import Marshmallow
from marshmallow_sqlalchemy import ModelSchema
from marshmallow import fields, validate, ValidationError
from flask_sqlalchemy import SQLAlchemy
from pprint import pprint

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = "mysql://root:@127.0.0.1:3306/Northwind"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)
ma = Marshmallow(app)

# engine = create_engine("mysql://root:@127.0.0.1:3306/Northwind")


class Customers(db.Model):
    __tablename__ = "customers"

    def create(self):
      db.session.add(self)
      db.session.commit()
      return self

    customerId = db.Column(db.String(25), primary_key=True)
    companyName = db.Column(db.String(25))
    address = db.Column(db.String(25))
    contactTitle = db.Column(db.String(25))
    address = db.Column(db.String(25))
    city = db.Column(db.String(25))
    region = db.Column(db.String(25))
    postalCode = db.Column(db.String(25))
    country = db.Column(db.String(25))
    phone = db.Column(db.String(25))
    fax = db.Column(db.String(25))

class CustomersSchema(ModelSchema):
    class Meta(ModelSchema.Meta):
        model = Customers
        sqla_session = db.session
        
    customerId = fields.Str()
    companyName = fields.Str()
    address = fields.Str()
    contactTitle = fields.Str()
    address = fields.Str()
    city = fields.Str()
    region = fields.Str()
    postalCode = fields.Str(validate=validate.Length(min=1))
    country = fields.Str()
    phone = fields.Str(validate=validate.Length(min=10,max=10))
    fax = fields.Str()

''' Other schema definitions here'''

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@app.route('/customers/get', methods=['GET'])
def getCustomers():
    query_parameters = request.args
    query_data = query_parameters.to_dict()
    customerId = query_data['customerId']

    try:
        data = Customers().query.get(customerId)
        customers_schema = CustomersSchema() # Self Note : Removing many=True made it work
        customers_data = customers_schema.dump(data)

        return make_response({'data':customers_data}, 200)
    except Exception as err:
        pprint(err)
        return page_not_found(404)


@app.route('/customers/post', methods=['POST'])
def postCustomers():
    query_parameters = request.args
    query_data = query_parameters.to_dict()

    try:
        customers_schema = CustomersSchema()
        customers = customers_schema.load(query_data)

        result = customers_schema.dump(customers.create())

        return make_response({'message' :result},200)
    except ValidationError as err:
        pprint(err.messages)
        message = {'message': err.messages}
        return make_response(jsonify(message), 400)
    except Exception as err:
        pprint(err)
        return make_response({'error':'Some Error'}, 400)


@app.route('/customers/put', methods=['PUT'])
def putCustomers():
    query_parameters = request.args
    query_data = query_parameters.to_dict()

    try:
        customerId = query_data['customerId']
        customer_db = Customers().query.get(customerId)

        if query_data['customerId']:
            customer_db.customerId = query_data['customerId']
        if query_data['companyName']:
            customer_db.companyName = query_data['companyName']
        if query_data['contactName']:
            customer_db.contactName = query_data['contactName']
        if query_data['contactTitle']:
            customer_db.contactTitle = query_data['contactTitle']
        if query_data['address']:
            customer_db.address = query_data['address']
        if query_data['city']:
            customer_db.city = query_data['city']
        if query_data['region']:
            customer_db.region = query_data['region']
        if query_data['postalCode']:
            customer_db.postalCode = query_data['postalCode']
        if query_data['country']:
            customer_db.country = query_data['country']
        if query_data['phone']:
            customer_db.phone = query_data['phone']
        if query_data['fax']:
            customer_db.fax = query_data['fax']
        
        db.session.add(customer_db)
        db.session.commit()
        
        customer_schema = CustomersSchema()
        result = customer_schema.dump(customer_db)
        
        return make_response({'message':'Added Succesfuly', 'data': result},200)
    except Exception as err:
        pprint(err)
        return make_response({'error':'Some error'}, 400)

app.run(debug=True)