export interface User extends Document {
  name: string;
  email: string;
  password: string;
  role: string;
  createdAt: Date;
}

export interface UserDocument extends User {
  createdAt: Date;
  updatedAt: Date;
  _doc?: any;
}
