from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

# Configuration for the SQLite database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# User model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        # Fetch all users
        users = User.query.all()
        users_list = [
            {'id': user.id, 'username': user.username, 'email': user.email}
            for user in users
        ]
        return jsonify(users_list)

    if request.method == 'POST':
        data = request.get_json()

        # Check if username or email already exists
        existing_user = User.query.filter(
            (User.username == data['username']) | (User.email == data['email'])
        ).first()
        
        if existing_user:
            return jsonify({'message': 'Username or email already exists'}), 400

        # Hash the password and create a new user
        hashed_password = generate_password_hash(data['password'], method='sha256')
        new_user = User(username=data['username'], email=data['email'], password=hashed_password)
        db.session.add(new_user)
        db.session.commit()
        return jsonify({'message': 'User registered successfully!'})


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    user = User.query.filter_by(email=data['email']).first()
    if not user or not check_password_hash(user.password, data['password']):
        return jsonify({'message': 'Invalid email or password'}), 401
    return jsonify({'message': 'Logged in successfully!'})

@app.route('/update_user/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    user = User.query.get(user_id)

    if not user:
        return jsonify({'message': 'User not found'}), 404

    if 'username' in data:
        user.username = data['username']
    if 'email' in data:
        existing_user = User.query.filter_by(email=data['email']).first()
        if existing_user and existing_user.id != user_id:
            return jsonify({'message': 'Email already in use'}), 400
        user.email = data['email']

    db.session.commit()
    return jsonify({'message': 'User updated successfully!'})

@app.route('/delete_user/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    user = User.query.get(user_id)

    if not user:
        return jsonify({'message': 'User not found'}), 404

    db.session.delete(user)
    db.session.commit()
    return jsonify({'message': 'User deleted successfully!'})

@app.route('/get_user/<int:user_id>', methods=['GET'])
def get_user(user_id):
    user = User.query.get(user_id)

    if not user:
        return jsonify({'message': 'User not found'}), 404

    user_data = {'id': user.id, 'username': user.username, 'email': user.email}
    return jsonify(user_data)

@app.route('/change_password/<int:user_id>', methods=['PUT'])
def change_password(user_id):
    data = request.get_json()
    user = User.query.get(user_id)

    if not user:
        return jsonify({'message': 'User not found'}), 404

    if not check_password_hash(user.password, data['old_password']):
        return jsonify({'message': 'Old password is incorrect'}), 401

    hashed_password = generate_password_hash(data['new_password'], method='sha256')
    user.password = hashed_password
    db.session.commit()
    return jsonify({'message': 'Password changed successfully!'})

@app.route('/search_user', methods=['GET'])
def search_user():
    query = request.args.get('query', '')
    users = User.query.filter(
        (User.username.like(f'%{query}%')) | (User.email.like(f'%{query}%'))
    ).all()

    users_list = [
        {'id': user.id, 'username': user.username, 'email': user.email}
        for user in users
    ]
    return jsonify(users_list)

@app.route('/logout', methods=['POST'])
def logout():
    # For now, just return a message
    return jsonify({'message': 'Logged out successfully!'})

if __name__ == '__main__':
    # Ensure the application context is used for database creation
    with app.app_context():
        db.create_all()  # Create the database and tables
    app.run(debug=True)
