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
    plate = db.Column(db.String(50))
    machine_number = db.Column(db.String(50))
    skeleton_number = db.Column(db.String(50))
    
    def to_dict(self):
        data = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        # Remove fields that should not be in the response
        for field in ['plate', 'machine_number', 'skeleton_number']:
            data.pop(field, None)
        return data

@app.route('/violations', methods=['GET'])
def get_violations():
    plate = request.args.get('plate')
    machine_number = request.args.get('machine_number')
    skeleton_number = request.args.get('skeleton_number')

    if not all([plate, machine_number, skeleton_number]):
        return jsonify({'error': 'Missing required parameters'}), 400

    violations_query = Violation.query.filter_by(
        plate=plate,
        machine_number=machine_number,
        skeleton_number=skeleton_number
    ).all()

    all_violations = [violation.to_dict() for violation in violations_query]
    return jsonify(all_violations)

if __name__ == '__main__':
    app.run(debug=True)
