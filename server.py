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

class BytesField(fields.Field):
    def _validate(self, value):
        if not isinstance(value, bytes):
            raise ValidationError('Invalid input type.')

        if value is None or value == b'':
            raise ValidationError('Invalid value')

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

class Products(db.Model):
    __tablename__ = "products"

    def create(self):
      db.session.add(self)
      db.session.commit()
      return self

    productId = db.Column(db.Integer, primary_key=True)
    productName = db.Column(db.String(25))
    supplierId = db.Column(db.Integer)
    categoryId = db.Column(db.Integer)
    quantityPerUnit = db.Column(db.String(25))
    unitPrice = db.Column(db.Numeric(10,4))
    unitsInStock = db.Column(db.Integer)
    unitsOnOrder = db.Column(db.Integer)
    recorderLevel = db.Column(db.Integer) # recorder - spellinhg mistake while entering column in mysql
    discontinued = db.Column(db.String(5)) #Bit type

class ProductsSchema(ModelSchema):
    class Meta(ModelSchema.Meta):
        model = Products
        sqla_session = db.session
    
    productId = fields.Int()
    productName = fields.Str()
    categoryId = fields.Int()
    supplierId = fields.Int()
    quantityPerUnit = fields.Str()
    unitPrice = fields.Float()
    unitsInStock = fields.Int()
    unistOnOrder = fields.Int()
    recorderLevel = fields.Int() # recorder - spellinhg mistake while entering column in mysql
    discontinued = fields.Str()
    
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
        customers_data = customers_schema.dump(data) # De-serialize

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
        if customer_db is None:
            raise Exception('No such entry in Database')

        if 'customerId' in query_data:
            customer_db.customerId = query_data['customerId']
        if 'companyName' in query_data:
            customer_db.companyName = query_data['companyName']
        if 'contactName' in query_data:
            customer_db.contactName = query_data['contactName']
        if 'contactTitle' in query_data:
            customer_db.contactTitle = query_data['contactTitle']
        if 'address' in query_data:
            customer_db.address = query_data['address']
        if 'city' in query_data:
            customer_db.city = query_data['city']
        if 'region' in query_data:
            customer_db.region = query_data['region']
        if 'postalCode' in query_data:
            customer_db.postalCode = query_data['postalCode']
        if 'country' in query_data:
            customer_db.country = query_data['country']
        if 'phone' in query_data:
            customer_db.phone = query_data['phone']
        if 'fax' in query_data:
            customer_db.fax = query_data['fax']
        
        db.session.add(customer_db)
        db.session.commit()
        
        customer_schema = CustomersSchema()
        result = customer_schema.dump(customer_db)
        
        return make_response({'message':'Added Succesfuly', 'data': result},200)
    except Exception as err:
        pprint(err)
        return make_response({'error':'Some error'}, 400)


@app.route('/products/get', methods=['GET'])
def getProducts():
    query_parameters = request.args
    query_data = query_parameters.to_dict()
    productId = query_data['productId']

    try:        
        data = Products().query.get(productId)
        print(data)
        
        products_schema = ProductsSchema() # Self Note : Removing many=True made it work
        
        products_data = products_schema.dump(data) # De-serialize
        
        return make_response({'data':products_data}, 200)
    except Exception as err:
        pprint(err)
        return page_not_found(404)


@app.route('/products/post', methods=['POST'])
def postProducts():
    query_parameters = request.args
    query_data = query_parameters.to_dict()

    try:
        products_schema = ProductsSchema()
        products = products_schema.load(query_data)

        result = products_schema.dump(products.create())

        return make_response({'message' :result},200)
    except ValidationError as err:
        pprint(err.messages)
        message = {'message': err.messages}
        return make_response(jsonify(message), 400)
    except Exception as err:
        pprint(err)
        return make_response({'error':'Some Error'}, 400)


@app.route('/products/put', methods=['PUT'])
def putProducts():
    query_parameters = request.args
    query_data = query_parameters.to_dict()

    try:
        
        productId = query_data['productId']
        product_db = Products().query.get(productId)
        if product_db is None:
            raise Exception('No such entry in Database')

        if 'productId' in query_data:
            product_db.productId = query_data['productId']
        if 'productName' in query_data:
            product_db.productName = query_data['productName']
        if 'categoryId' in query_data:
            product_db.categoryId = query_data['categoryId']
        if 'supplierId' in query_data:
            product_db.supplierId = query_data['supplierId']
        if 'quantityPerUnit' in query_data:
            product_db.quantityPerUnit = query_data['quantityPerUnit']
        if 'unitPrice' in query_data:
            product_db.unitPrice = query_data['unitPrice']
        if 'unitsInStock' in query_data:
            product_db.unitsInStock = query_data['unitsInStock']
        if 'unitsOnOrder' in query_data:
            product_db.unitsOnOrder = query_data['unitsOnOrder']
        if 'recorderLevel' in query_data:
            product_db.recorderLevel = query_data['recorderLevel']
        if 'discontinued' in query_data:
            product_db.discontinued = query_data['discontinued']
        
        db.session.add(product_db)
        db.session.commit()
        
        product_schema = ProductsSchema()
        result = product_schema.dump(product_db)
        
        return make_response({'message':'Added Succesfuly', 'data': result},200)
    except Exception as err:
        pprint(err)
        return make_response({'error':'Some error'}, 400)

app.run(debug=True)