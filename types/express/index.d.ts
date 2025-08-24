interface UserPayload {
  id: string;
  role: {
    Admin: boolean;
    Creative: boolean;
    Client: boolean;
  };
}

declare namespace Express {
  interface Request {
    user: UserPayload;
    files?: {
      [fieldname: string]: Express.Multer.File[];
    };
  }
}
