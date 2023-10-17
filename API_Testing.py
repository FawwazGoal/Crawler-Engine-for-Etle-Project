from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# Update this URI with your actual PostgreSQL credentials
DATABASE_URI = 'postgresql+pg8000://postgres:Fawwaz410133@localhost/etle_violations'

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Silence the deprecation warning

db = SQLAlchemy(app)

class Violation(db.Model):
    __tablename__ = 'violations'
    id = db.Column(db.Integer, primary_key=True)
    location = db.Column(db.String(50))
    penalty_type_id = db.Column(db.Integer, db.ForeignKey('penalty_types.id'))
    penalty_type_en = db.Column(db.String(50))
    
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

@app.route('/violations', methods=['GET'])
def get_violations():
    plate = request.args.get('plate')
    machine_number = request.args.get('machine_number')
    skeleton_number = request.args.get('skeleton_number')

    if not all([plate, machine_number, skeleton_number]):
        return jsonify({'error': 'Missing required parameters'}), 400

    # This is a placeholder for now, you'll need to query your database based on these parameters
    violations_query = Violation.query.all()

    all_violations = [violation.to_dict() for violation in violations_query]
    return jsonify(all_violations)

if __name__ == '__main__':
    app.run(debug=True)
