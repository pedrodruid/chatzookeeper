package chatzookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class chat implements Watcher, StatCallback{

	static ZooKeeper zk = null;
	static final String root = "/chat3";

	String msg;
	String cliente;	
	String node;
	Stat s = null;
	String temp = new String("");

	chat(String cliente) {
		this.cliente = cliente;
		if (zk == null) {
			try {
				System.out.println("Starting ZK:");
				zk = new ZooKeeper("127.0.0.1", 2181, this);
				s = zk.exists(root, true);	
				if (s != null) {
					byte[] data = zk.getData(root, this, s);
				}
			} catch (IOException e) {
				System.out.println(e.toString());
				zk = null;
			} catch (KeeperException e) {
				System.out.println(e.toString());
				e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println(e.toString());
				e.printStackTrace();
			}
		}
	}

	public void send(String msg) {
		if (zk != null) {
			try {
				s = zk.exists(root, true);
				if (s == null) { //se não existe 'chat' criado, então cria o chat
					zk.create(root, msg.getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				} else { //caso já exista chat, apenas envia mensagem
					zk.setData(root, msg.getBytes(), s.getVersion());
				}
			} catch (KeeperException e) {
				System.out
						.println("Keeper exception when instantiating queue: "
								+ e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted exception");
			}
		}
	}



	synchronized public void run() {
		while (true) { //laço infinito de envi de mensagem
			System.out.println("Mensagem:");
			Scanner scan = new Scanner(System.in);

			temp = cliente + ": " + scan.nextLine();
			this.send(temp);		
		}
	}

	public static void main(String args[]) {
		if (args.length == 0) {
			System.out.println("O nome deve ser informado via parametro.");
	        System.exit(0);
		}
		chat c = new chat(args[0]);
		c.run();
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		switch (watchedEvent.getType()) {
		case None:
			processNoneEvent(watchedEvent);
			break;
		case NodeDeleted:
			break;
		case NodeDataChanged:
			try {
				if (s != null) { //pega a mensagem enviada e exibi no console
					byte[] data = zk.getData(root, this, s);
					String msg = new String(data);
					if (!temp.equals(msg)) {
						System.out.println(msg);
					}
				}
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case NodeChildrenChanged:
			break;
		case NodeCreated:
			break;
		} 
	}


	public void processNoneEvent(WatchedEvent event) {
		switch (event.getState()) {
		case SyncConnected:
			System.out.println("none event+ sync connected");
			break;
		case AuthFailed:
			System.out.println("none event+  AuthFailed");
			break;
		case Disconnected:
			System.out.println("none event+  Disconnected");
			break;
		case Expired:
			System.out.println("none event+  Expired");
			break;
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		// TODO Auto-generated method stub

	}

}
