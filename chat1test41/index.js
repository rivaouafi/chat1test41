const express = require('express');
const { createServer } = require('node:http');
const { join } = require('node:path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { availableParallelism } = require('node:os');
const cluster = require('node:cluster');
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // create one worker per available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i
    });
  }

  // set up the adapter on the primary thread
  return setupPrimary();
}

async function main() {
  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    // set up the adapter on each worker thread
    adapter: createAdapter()
  });

  // [...]

  // each worker will listen on a distinct port
  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}

main();



 io.on('connection', (socket) => {
  console.log('a user connected');
  socket.on('disconnect', () => {
    console.log('user disconnected');
  });
});

  io.on('connection', (socket) => {
  socket.on('chat message', (msg) => {
    console.log('message: ' + msg);
  });
});


  // this will emit the event to all connected sockets
io.emit('hello', 'world');

 io.on('connection', (socket) => {
  socket.broadcast.emit('hi');
});

 io.on('connection', (socket) => {
  socket.on('chat message', (msg) => {
    io.emit('chat message', msg);
  });
});

 socket.emit('hello', 'world');

 io.on('connection', (socket) => {
  socket.on('hello', (arg) => {
    console.log(arg); // 'world'
  });
});

 io.on('connection', (socket) => {
  socket.emit('hello', 'world');
});

  socket.on('hello', (arg) => {
  console.log(arg); // 'world'
});


 socket.emit('hello', 1, '2', { 3: '4', 5: Uint8Array.from([6]) });

  io.on('connection', (socket) => {
  socket.on('hello', (arg1, arg2, arg3) => {
    console.log(arg1); // 1
    console.log(arg2); // '2'
    console.log(arg3); // { 3: '4', 5: <Buffer 06> }
  });
});


 io.on('connection', (socket) => {
  socket.emit('hello', 1, '2', { 3: '4', 5: Buffer.from([6]) });
});


socket.on('hello', (arg1, arg2, arg3) => {
  console.log(arg1); // 1
  console.log(arg2); // '2'
  console.log(arg3); // { 3: '4', 5: ArrayBuffer (1) [ 6 ] }
});

 socket.timeout(5000).emit('request', { foo: 'bar' }, 'baz', (err, response) => {
  if (err) {
    // the server did not acknowledge the event in the given delay
  } else {
    console.log(response.status); // 'ok'
  }
});

 io.on('connection', (socket) => {
  socket.on('request', (arg1, arg2, callback) => {
    console.log(arg1); // { foo: 'bar' }
    console.log(arg2); // 'baz'
    callback({
      status: 'ok'
    });
  });
});

io.on('connection', (socket) => {
  socket.timeout(5000).emit('request', { foo: 'bar' }, 'baz', (err, response) => {
    if (err) {
      // the client did not acknowledge the event in the given delay
    } else {
      console.log(response.status); // 'ok'
    }
  });
});


 socket.on('request', (arg1, arg2, callback) => {
  console.log(arg1); // { foo: 'bar' }
  console.log(arg2); // 'baz'
  callback({
    status: 'ok'
  });
});


 try {
  const response = await socket.timeout(5000).emitWithAck('request', { foo: 'bar' }, 'baz');
  console.log(response.status); // 'ok'
} catch (e) {
  // the server did not acknowledge the event in the given delay
}

 io.on('connection', (socket) => {
  socket.on('request', (arg1, arg2, callback) => {
    console.log(arg1); // { foo: 'bar' }
    console.log(arg2); // 'baz'
    callback({
      status: 'ok'
    });
  });
});

io.on('connection', async (socket) => {
  try {
    const response = await socket.timeout(5000).emitWithAck('request', { foo: 'bar' }, 'baz');
    console.log(response.status); // 'ok'
  } catch (e) {
    // the client did not acknowledge the event in the given delay
  }
});


 socket.on('request', (arg1, arg2, callback) => {
  console.log(arg1); // { foo: 'bar' }
  console.log(arg2); // 'baz'
  callback({
    status: 'ok'
  });
});

 socket.emit('hello', 1, '2', { 3: '4', 5: Uint8Array.from([6]) });


  socket.onAny((eventName, ...args) => {
  console.log(eventName); // 'hello'
  console.log(args); // [ 1, '2', { 3: '4', 5: ArrayBuffer (1) [ 6 ] } ]
});

 socket.onAnyOutgoing((eventName, ...args) => {
  console.log(eventName); // 'hello'
  console.log(args); // [ 1, '2', { 3: '4', 5: ArrayBuffer (1) [ 6 ] } ]
});


 io.on('connection', (socket) => {
  // join the room named 'some room'
  socket.join('some room');

  // broadcast to all connected clients in the room
  io.to('some room').emit('hello', 'world');

  // broadcast to all connected clients except those in the room
  io.except('some room').emit('hello', 'world');

  // leave the room
  socket.leave('some room');
});





 function emit(socket, event, arg) {
  socket.timeout(5000).emit(event, arg, (err) => {
    if (err) {
      // no ack from the server, let's retry
      emit(socket, event, arg);
    }
  });
}

emit(socket, 'hello', 'world');

 io.on('connection', (socket) => {
  socket.on('hello', (value, callback) => {
    // once the event is successfully handled
    callback();
  });
})





// [...]

io.on('connection', async (socket) => {
  socket.on('chat message', async (msg, clientOffset, callback) => {
    let result;
    try {
      result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', msg, clientOffset);
    } catch (e) {
      if (e.errno === 19 /* SQLITE_CONSTRAINT */ ) {
        // the message was already inserted, so we notify the client
        callback();
      } else {
        // nothing to do, just let the client retry
      }
      return;
    }
    io.emit('chat message', msg, result.lastID);
    // acknowledge the event
    callback();
  });

  if (!socket.recovered) {
    try {
      await db.each('SELECT id, content FROM messages WHERE id > ?',
        [socket.handshake.auth.serverOffset || 0],
        (_err, row) => {
          socket.emit('chat message', row.content, row.id);
        }
      )
    } catch (e) {
      // something went wrong
    }
  }
});

// [...]