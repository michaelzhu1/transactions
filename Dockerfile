FROM node:carbon

WORKDIR /Users/michaelzhu/Desktop/transactions

COPY package*.json ./

RUN npm install

COPY . .

CMD [ "npm", "run", "start", "--silent" ]
