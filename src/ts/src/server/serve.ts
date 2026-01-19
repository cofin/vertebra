import { createAsgiServer } from "./index";

const server = await createAsgiServer();
server.start();
