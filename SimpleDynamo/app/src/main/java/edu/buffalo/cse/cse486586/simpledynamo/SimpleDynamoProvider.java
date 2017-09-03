package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	List<String> nodeList = new ArrayList<String>();
	//	List<String> nodes = new ArrayList<String>();
	TreeMap<String, String> nodeHash = new TreeMap();
	Map<String, Boolean> nodeMap = new HashMap<String, Boolean>();
	Map<String, String> predList = new HashMap<String, String>();
	Map<String, String> succList = new HashMap<String, String>();
	static public ReentrantLock lock = new ReentrantLock();
	static public ReentrantLock lock1 = new ReentrantLock();

	boolean recovering = false;
	boolean inserting = false;
	boolean querying = false;
	String selfID;
	String selfPort;
	String succ1,succ2,pred1,pred2;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String query = selection;
		if(query.contains("@")){
			System.out.println("Received Delete @ at "+selfID);
			String[] fileArray = getContext().getFilesDir().list();
			for (String file : fileArray) {
//                    try {
				if(file.equals("Check")){
					continue;
				}
				System.out.println("Deleting "+file+" at "+selfID);

				getContext().deleteFile(file);
			}

		}
		else if(query.contains("*")){
			String[] fileArray = getContext().getFilesDir().list();
			for (String file : fileArray) {
//                    try {
				if(file.equals("Check")){
					continue;
				}
				getContext().deleteFile(file);
			}

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETEALL");


		}
		else{
			String keyHash;
			try {
				keyHash = genHash(query);

				String pos = getPos(keyHash);
				String avd = nodeHash.get(pos);

				if(avd.equals(selfID)){
					System.out.println("Deleting "+query + " at " + selfID);
					getContext().deleteFile(query);
					String msgToDel = query+":"+succ1;

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETEREDIR",msgToDel);

					msgToDel = query+":"+succ2;

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETEREDIR",msgToDel);



				}
				else{
					String[] back = succList.get(avd).split(":");
					System.out.println("Sending Delete req for "+query + " to " + avd);
					String msgToDel = query+":"+avd;

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETEREDIR",msgToDel);
					if(back[0].equals(selfID)){
						getContext().deleteFile(query);

					}
					else {
						msgToDel = query + ":" + back[0];

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETEREDIR", msgToDel);
					}

					if(back[1].equals(selfID)){
						getContext().deleteFile(query);

					}
					else {

						msgToDel = query + ":" + back[1];

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETEREDIR", msgToDel);
					}


				}



			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public  Uri insert(Uri uri, ContentValues values) {
		inserting = true;
		while(recovering){
			System.out.println("Recovering ON? "+recovering);

			try {
				Thread.sleep(600);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String key = values.getAsString(KEY_FIELD);
		String val = values.getAsString(VALUE_FIELD);
		System.out.println("INSERT: Key : value "+key+" "+val);
//		lock.lock();
		try {
			String keyHash = genHash(key);
			String keyPos = getPos(keyHash);
			String avdPos = nodeHash.get(keyPos);
			String sucStr = succList.get(avdPos);
			String[] reps = sucStr.split(":");
			String rep1 = reps[0];
			String rep2 = reps[1];
			System.out.println("Msg AVD is "+avdPos);
			if (avdPos.equals(selfID)) {
//				lock1.lock();
//
//				lock.lock(); System.out.println("Locked at "+selfID);
				FileOutputStream outputStream;
				String msg = values.get("value").toString();

				try {
					outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
					outputStream.write(val.getBytes());
					outputStream.close();
					Log.v("insert", key+" : "+val);
					String message = key + ":" + val + ":" + rep1;
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "REPLICATE", message);
					System.out.println("Sent Replica from " + selfID + " to " + rep1 + " for "+key);
					String message1 = key + ":" + val + ":" + rep2;
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "REPLICATE", message1);
					System.out.println("Sent Replica from " + selfID + " to " + rep2 + " for "+key);

				} catch (Exception e) {
					//  Log.e(TAG, "File write failed");
				}finally {
//					lock.unlock(); System.out.println("Unlocked at "+selfID);
//					lock1.unlock();
					Thread.sleep(1000);
					inserting = false;
				}




			}
			else {
//				lock.lock(); System.out.println("Locked at "+selfID);
//				lock1.lock();
//				lock.lock();
				try{

					String message = key + ":" + val + ":" + avdPos;
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT", message);
					System.out.println("Sent Insert Redir from " + selfID + " to " + avdPos + " for " + key);

				}finally {
//					lock.unlock();
//					lock1.unlock();
				}
				Thread.sleep(1000);

				inserting = false;
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
//			lock.unlock();
		}


		// TODO Auto-generated method stub
		return null;
	}

	//http://stackoverflow.com/questions/6343166/how-to-fix-android-os-networkonmainthreadexception


	public String getPos(String s) {
		//https://docs.oracle.com/javase/7/docs/api/java/util/TreeMap.html

		String result;
		//Check if keyHash is greater than max AVD hash present in Ring
		if (s.compareTo(nodeHash.lastKey()) > 0) {
			result = nodeHash.firstKey();
//			System.out.println("Returning getPos firstKey " + result);

			return result;

		} else {
			//Return hash of AVD having least hash greater than or equal to the given key, or null if there is no such key.
			result = nodeHash.ceilingKey(s);
//			System.out.println("Returning getPos ceilingKey " + result);
			return result;
		}


	}
//	public synchronized String recoverPred(String port,String avd1,String avd2){

	public  String recoverPred(String port,String avd1,String avd2){

		String msg = "RECMW"+":"+avd1+":"+avd2+":";

		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					(Integer.parseInt(port) * 2));
//			socket.setSoTimeout(6000);
			PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
			pw.println(msg);
			Thread.sleep(50);
			InputStreamReader is = new InputStreamReader(socket.getInputStream());
			BufferedReader br1 = new BufferedReader(is);
			String rep;
			if ((rep = br1.readLine()) != null) {
				int count = Integer.parseInt(rep);
//				System.out.println("Received count for QUERYALL " + count);
				String line;
				while((line=br1.readLine())!=null){
//						line = br1.readLine();


					String[] tab = line.split(":");
//					String path = getContext().getFilesDir().getAbsolutePath() + "/" + tab[0];
//					File file = new File(path);
//					if(file.exists()){
//						continue;
//					}
					FileOutputStream outputStream;
					String val = tab[1];

					try {
						System.out.println("Recovered key "+tab[0] + " from " + port);
						outputStream = getContext().openFileOutput(tab[0], Context.MODE_PRIVATE);
						outputStream.write(val.getBytes());
						outputStream.close();
						Log.v("insert", tab[0]+" : "+val);

					} catch (Exception e) {
						//  Log.e(TAG, "File write failed");
					}
//						System.out.println("Adding row to res for "+node+" key is "+tab[0]+" value is "+tab[1]);
//						res.addRow(new Object[]{tab[0], tab[1]});
				}
			}
			br1.close();
			pw.close();
			socket.close();
//			int count = Integer.parseInt(br1.readLine());

		} catch(SocketTimeoutException ie){
			System.out.println("Socket failure during recovery for port "+port);
			return "false";
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
			System.out.println("Socket failure during recovery for port "+port);
			return "false";
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Socket failure during recovery for port "+port);
			return "false";
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		return "true";
	}

	public String recoverNew(String port){

		String msg = "RECOVERALL";
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					(Integer.parseInt(port) * 2));
			socket.setSoTimeout(1000);
			PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
			pw.println(msg);

//			Thread.sleep(50);

			InputStreamReader is = new InputStreamReader(socket.getInputStream());
			BufferedReader br1 = new BufferedReader(is);
			String reply;
			if((reply = br1.readLine())!=null){
				System.out.println("Port "+port+" Returning recovery msg "+reply);
				return reply;
			}else {
				System.out.println("Return fail "+reply);
				return null;
			}

		} catch (SocketTimeoutException e){
			System.out.println("Socket time out for port "+port);
			return null;
		}catch (UnknownHostException e) {

			e.printStackTrace();
			System.out.println("Socket time out for port "+port);
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Socket time out for port "+port);
			return null;
		}
		catch (Exception e){
			System.out.println("Socket time out for port "+port);
			return null;
		}
//		return "true";
//		return null;
	}

//	public synchronized String recoverSucc(String port,String avd1){

	public  String recoverSucc(String port,String avd1){

		String msg = "RECSELF"+":"+avd1+":";

		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					(Integer.parseInt(port) * 2));
//			socket.setSoTimeout(6000);
			PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
			pw.println(msg);
			Thread.sleep(50);
			InputStreamReader is = new InputStreamReader(socket.getInputStream());
			BufferedReader br1 = new BufferedReader(is);
			String rep;
			if ((rep = br1.readLine()) != null) {
				int count = Integer.parseInt(rep);
//				System.out.println("Received count for QUERYALL " + count);
				String line;
				while((line=br1.readLine())!=null){
//						line = br1.readLine();


					String[] tab = line.split(":");
//					String path = getContext().getFilesDir().getAbsolutePath() + "/" + tab[0];
//					File file = new File(path);
//					if(file.exists()){
//						continue;
//					}
					FileOutputStream outputStream;
					String val = tab[1];

					try {
						System.out.println("Recovered key "+tab[0] + " from " + port);
						outputStream = getContext().openFileOutput(tab[0], Context.MODE_PRIVATE);
						outputStream.write(val.getBytes());
						outputStream.close();
						Log.v("insert", tab[0]+" : "+val);

					} catch (Exception e) {
						//  Log.e(TAG, "File write failed");
					}
//						System.out.println("Adding row to res for "+node+" key is "+tab[0]+" value is "+tab[1]);
//						res.addRow(new Object[]{tab[0], tab[1]});
				}
			}
			br1.close();
			pw.close();
			socket.close();
//			int count = Integer.parseInt(br1.readLine());

		} catch(SocketTimeoutException ie){
			System.out.println("Socket failure during recovery for port "+port);
			return "false";
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
			System.out.println("Socket failure during recovery for port "+port);
			return "false";
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Socket failure during recovery for port "+port);
			return "false";
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		return "true";
	}



	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		nodeList.add("5562");
		nodeList.add("5556");
		nodeList.add("5554");
		nodeList.add("5558");
		nodeList.add("5560");
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		selfPort = myPort;
		selfID = portStr;

		succ1 = nodeList.get((nodeList.indexOf(selfID)+1)%5);
		succ2 = nodeList.get((nodeList.indexOf(selfID)+2)%5);
		pred1 = nodeList.get((nodeList.indexOf(selfID)+4)%5);
		pred2 = nodeList.get((nodeList.indexOf(selfID)+3)%5);

		String path = getContext().getFilesDir().getAbsolutePath() + "/" + "Check";


		File file = new File(path);
		if(file.exists()){
//			lock.lock(); System.out.println("Locked at "+selfID);
			System.out.println("File Exists");

			//Delete all Local Files
			String[] fileArray = getContext().getFilesDir().list();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "RECOVER");

			for (String file1 : fileArray) {
//                    try {
				if(file1.equals("Check")){
					continue;
				}
				else {
//					System.out.println("OnCreate Deleting " + file1 + " at "+selfID);
					getContext().deleteFile(file1);
				}
			}
			System.out.println("Starting recovery");
//			String msg = "RECOVER";

//			String predRec = recoverPred(pred1,pred1,pred2);
//			if(predRec.equals("false")){
//				recoverPred(pred2,pred1,pred2);
//			}
//			String succRec = recoverSucc(succ1,selfID);
//			if(succRec.equals("fail")){
//				recoverSucc(succ2,selfID);
//			}



//			lock.unlock(); System.out.println("Unlocked at "+selfID);
//			String[] fileArray1 = getContext().getFilesDir().list();
//			System.out.println("Number of files after recovery is "+(fileArray1.length -1));

			//Broadcast your recovery
			//Start Recovery of missed writes/replication

		}else {
			try {
				FileOutputStream fos;
				fos = getContext().openFileOutput("Check", Context.MODE_PRIVATE);
				fos.write("first".getBytes());
				fos.close();
				System.out.println("File doesn't exist");
			} catch (Exception e) {
				Log.e(TAG, "File write failed");
			}
		}
		try {

			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

		}

		try {
			nodeHash.put(genHash(nodeList.get(0)),nodeList.get(0));
			nodeHash.put(genHash(nodeList.get(1)),nodeList.get(1));
			nodeHash.put(genHash(nodeList.get(2)),nodeList.get(2));
			nodeHash.put(genHash(nodeList.get(3)),nodeList.get(3));
			nodeHash.put(genHash(nodeList.get(4)),nodeList.get(4));
			//System.out.println("TreeMap is "+nodeHash);
			nodeMap.put("5562",true);
			nodeMap.put("5556",true);
			nodeMap.put("5554",true);
			nodeMap.put("5558",true);
			nodeMap.put("5560",true);
			predList.put("5562","5560:5558");
			predList.put("5556","5562:5560");
			predList.put("5554","5556:5562");
			predList.put("5558","5554:5556");
			predList.put("5560","5558:5554");
			succList.put("5562","5556:5554");
			succList.put("5556","5554:5558");
			succList.put("5554","5558:5560");
			succList.put("5558","5560:5562");
			succList.put("5560","5562:5556");


		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}




		//http://stackoverflow.com/questions/10576930/trying-to-check-if-a-file-exists-in-internal-storage



		return false;
	}

	@Override
	public  Cursor query(Uri uri, String[] projection, String selection,
						 String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		querying=true;
		while(recovering||inserting){
			System.out.println("Recovering ON? "+recovering);

			try {
				Thread.sleep(600);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
		String query = selection;
		System.out.println("Received query "+selection+" at "+selfID);

//		lock1.lock();
		try {
			if (query.equals("\"@\"") || query.equals("@")) {
//			lock.lock();

				System.out.println("Entering local @ query at " + selfID);
				String[] fileArray = getContext().getFilesDir().list();
				for (String file : fileArray) {
					try {
//                        FileInputStream fis = getContext().openFileInput(file);
//                        if(fis!=null){
//
//                        }
						if (file.equals("Check")) {
							continue;
						}
						String keyHash = genHash(file);
						String keyPos = getPos(keyHash);
						String avdPos = nodeHash.get(keyPos);
						if (avdPos.equals(selfID) || avdPos.equals(pred1) || avdPos.equals(pred2)) {


							BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(file)));
							String res;
							if ((res = br.readLine()) != null) {
								cursor.addRow(new Object[]{file, res});
							}
						}

					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}

				}
//			lock.unlock();
//				querying = false;
				return cursor;

			} else if (query.equals("\"*\"") || query.equals("*")) {
//			lock.lock();

				try {
					MatrixCursor resall = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYALL").get();

//				lock.unlock();
//					querying = false;

					return resall;

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
//			lock.unlock();

			} else {
//			lock.lock();

				try {
					String keyHash = genHash(query);
					String pos = getPos(keyHash);
					String avd = nodeHash.get(pos);
					System.out.println("Query AVD is " + avd + " selfID is " + selfID);
					String qsuc2 = succList.get(avd).split(":")[1];
					String qsuc1 = succList.get(avd).split(":")[0];

//					if ((avd.equals(selfID)) || avd.equals(pred1) || avd.equals(pred2))
//				if(avd.equals(selfID))
					if(qsuc2.equals(selfID))
					{
						System.out.println("Query " + query + " is in self");
						BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(query)));
						String res;
						if ((res = br.readLine()) != null) {
							System.out.println("Found file " + query + " in self");
							cursor.addRow(new Object[]{query, res});

						}
//						querying = false;

						return cursor;
					} else {
//						String message = query + ":" + selfID + ":" + avd;
						String message = query + ":" + selfID + ":" + qsuc2;

						MatrixCursor res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYREDIR", message).get();
//						querying = false;

						return res;


					}

				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
//			lock.unlock();


			}
		}finally {
querying=false;
		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private void selfRep(String key,String val){

		String message = key + ":" + val + ":" + succ1;
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "REPLICATE", message);
		String message1 = key + ":" + val + ":" + succ2;
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "REPLICATE", message1);
//						System.out.println("Sent Replica of " + avdPos + " from " + selfID + " to " + rep1 + " for " + key);



	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

//            try {
			//Open a server socket to listen to incoming messages
			while (true) {
//				lock.lock();
				try {
					Socket s = serverSocket.accept();
//                    ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

					InputStreamReader is = null;

					is = new InputStreamReader(s.getInputStream());
					BufferedReader br = new BufferedReader(is);
					String msg;
					if ((msg = br.readLine()) != null) {

						String[] msgAr = msg.split(":");
//
						if(msgAr[0].equals("RECOVERALL")){
							if(inserting||querying) Thread.yield();
							PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
							String reply = "init:";
							String[] fileArray = getContext().getFilesDir().list();
							for (String file : fileArray) {
								//                        FileInputStream fis = getContext().openFileInput(file);
//                        if(fis!=null){
								if (file.equals("Check")) {
									continue;
								}
								else {
//								String keyHash = genHash(file);
//								String keyPos = getPos(keyHash);
//								String avdPos = nodeHash.get(keyPos);
//								if (avdPos.equals(msgAr[1])||avdPos.equals(msgAr[2])) {
									try {
//                        }
										BufferedReader br2 = new BufferedReader(new InputStreamReader(getContext().openFileInput(file)));
										String res = br2.readLine();
//                                    if ((res = br2.readLine()) != null) {
//                                        cursor.addRow(new Object[]{file, res});
//										System.out.println("Sending value for * " + file + "  " + res);
//										pw.println(file + ":" + res);
//										pw.flush();
										reply = reply + file + ":" + res + ":";
									} catch (FileNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
//								}


							}
							pw.println(reply);
							pw.flush();
							System.out.println("Sent back recovery reply "+reply);

						}
						if (msgAr[0].equals("RECMW")) {
//							lock1.lock();
//							System.out.println("Locked at " + selfID);
							if(inserting||querying) Thread.yield();
							System.out.println("Received RECMW at " + selfID + " for " + msgAr[1]+" and "+msgAr[2]);
//
							PrintWriter pw = new PrintWriter(s.getOutputStream(), true);

							String[] fileArray = getContext().getFilesDir().list();
							//int count = fileArray.length;

							//pw.println(fileArray.length);
							//System.out.println("Sent count for * as "+count+" from "+selfID);
							//pw.flush();
							int count = fileArray.length;
							String test = Arrays.toString(fileArray);
							if (test.contains("Check")) {
								count = count - 1;
							}
							pw.println(count);
//							System.out.println("Sent count for * as " + count + " from " + selfID);
							pw.flush();
							for (String file : fileArray) {
								//                        FileInputStream fis = getContext().openFileInput(file);
//                        if(fis!=null){
								if (file.equals("Check")) {
									continue;
								}
								String keyHash = genHash(file);
								String keyPos = getPos(keyHash);
								String avdPos = nodeHash.get(keyPos);
								if (avdPos.equals(msgAr[1])||avdPos.equals(msgAr[2])) {
									try {
//                        }
										BufferedReader br2 = new BufferedReader(new InputStreamReader(getContext().openFileInput(file)));
										String res = br2.readLine();
//                                    if ((res = br2.readLine()) != null) {
//                                        cursor.addRow(new Object[]{file, res});
										System.out.println("Sending value for * " + file + "  " + res);
										pw.println(file + ":" + res);
										pw.flush();
									} catch (FileNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}

								}


							}
//							lock1.unlock();
//							System.out.println("Unlocked at " + selfID);


						}

						if (msgAr[0].equals("RECSELF")) {
//							lock1.lock();
//							System.out.println("Locked at " + selfID);

							System.out.println("Received RECSELF at " + selfID + " for " + msgAr[1]);
//							String URL = "content://edu.buffalo.cse.cse486586.simpledht.provider";
//							Uri uri = Uri.parse(URL);
//							ContentValues values = new ContentValues();
//							values.put(KEY_FIELD,msgAr[1]);
//							values.put(VALUE_FIELD,msgAr[2]);
//							insert(uri,values);
							PrintWriter pw = new PrintWriter(s.getOutputStream(), true);

							String[] fileArray = getContext().getFilesDir().list();
							//int count = fileArray.length;

							//pw.println(fileArray.length);
							//System.out.println("Sent count for * as "+count+" from "+selfID);
							//pw.flush();
							int count = fileArray.length;
							String test = Arrays.toString(fileArray);
							if (test.contains("Check")) {
								count = count - 1;
							}
							pw.println(count);
//							System.out.println("Sent count for * as " + count + " from " + selfID);
							pw.flush();
							for (String file : fileArray) {
								//                        FileInputStream fis = getContext().openFileInput(file);
//                        if(fis!=null){
								if (file.equals("Check")) {
									continue;
								}
								String keyHash = genHash(file);
								String keyPos = getPos(keyHash);
								String avdPos = nodeHash.get(keyPos);
								if (avdPos.equals(msgAr[1])) {
									try {
//                        }
										BufferedReader br2 = new BufferedReader(new InputStreamReader(getContext().openFileInput(file)));
										String res = br2.readLine();
//                                    if ((res = br2.readLine()) != null) {
//                                        cursor.addRow(new Object[]{file, res});
										System.out.println("Sending value for * " + file + "  " + res);
										pw.println(file + ":" + res);
										pw.flush();
									} catch (FileNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}

								}


							}
//							lock1.unlock();
//							System.out.println("Unlocked at " + selfID);


						}





//                            Uri newUri = getContentResolver().insert(uri,values);


						if (msgAr[0].equals("INSERT")) {
							while(recovering){
								Thread.yield();

							}
//							inserting = true;
//							lock1.tryLock();
//							System.out.println("Locked at " + selfID);
//							if(!lock1.isHeldByCurrentThread()){
//								PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
//								pw.println("Lock not acquired.Try Again");
//								System.out.println("Sent Lock not acquired");
//							}
							System.out.println("Received INSERT REDIR at " + selfID + " for key " + msgAr[1]);
//							String URL = "content://edu.buffalo.cse.cse486586.simpledht.provider";
//							Uri uri = Uri.parse(URL);
//							ContentValues values = new ContentValues();
//							values.put(KEY_FIELD,msgAr[1]);
//							values.put(VALUE_FIELD,msgAr[2]);
//							insert(uri,values);
//                            Uri newUri = getContentResolver().insert(uri,values);
							FileOutputStream outputStream;
							String val = msgAr[2];

							try {
								outputStream = getContext().openFileOutput(msgAr[1], Context.MODE_PRIVATE);
								outputStream.write(val.getBytes());
								outputStream.close();
								Log.v("insert", msgAr[1]+" : "+val);
								PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
								pw.println("Received Insert");
								System.out.println("Sent back ack for Insert" );
								selfRep(msgAr[1],val);



							} catch (Exception e) {
								//  Log.e(TAG, "File write failed");
							}
							finally {
//								lock1.unlock();
//								System.out.println("Unlocked at " + selfID);
							}
//						inserting = false;

						}
						if (msgAr[0].equals("REPLICATE")) {
							while(recovering){
								Thread.yield();

							}
//							inserting = true;
							System.out.println("Received Replicate at " + selfID + " for key " + msgAr[1]);

//							lock1.tryLock();
//							if(!lock1.isHeldByCurrentThread()){
//								PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
//								pw.println("Lock not acquired.Try Again");
//								System.out.println("Sent Lock not acquired");
//							}
//							System.out.println("Locked at " + selfID+ " for replicate");
//							String URL = "content://edu.buffalo.cse.cse486586.simpledht.provider";
//							Uri uri = Uri.parse(URL);
//							ContentValues values = new ContentValues();
//							values.put(KEY_FIELD,msgAr[1]);
//							values.put(VALUE_FIELD,msgAr[2]);
//							insert(uri,values);
//                            Uri newUri = getContentResolver().insert(uri,values);
							FileOutputStream outputStream;
							String val = msgAr[2];

							try {
								outputStream = getContext().openFileOutput(msgAr[1], Context.MODE_PRIVATE);
								outputStream.write(val.getBytes());
								outputStream.close();
								Log.v("insert", msgAr[1]+" : "+val);

								PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
								pw.println("Received Replicate");
								System.out.println("Sent back ack for replicate" );
							} catch (Exception e) {
								//  Log.e(TAG, "File write failed");
							}finally {
//								lock1.unlock();
//								inserting = false;

							}
//							lock1.unlock();
//							System.out.println("Replicate Unlocked at " + selfID);
						}
						if (msgAr[0].equals("QUERY")) {
							while(recovering||inserting){
								Thread.yield();
							}
//							lock1.lock();
							System.out.println("Query Locked at " + selfID);

							System.out.println("Received Query REDIR at " + selfID + " for key " + msgAr[1]);
							try{
								BufferedReader resp = new BufferedReader(new InputStreamReader(getContext().openFileInput(msgAr[1])));
								String res;
								if ((res = resp.readLine()) != null) {
									System.out.println("Successfully queried file");
									String response = msgAr[1] + ":" + res;
									PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
									pw.println(response);
									System.out.println("Sent back response to " + msgAr[2]);
//                                pw.flush();

//                                cursor.addRow(new Object[]{msg, res});
//                                return cursor;
								}
							}finally {
//								lock1.unlock();
								System.out.println("Query Unlocked at "+selfID);

							}
//                            Uri newUri = getContentResolver().insert(uri,values);

						}
						if (msgAr[0].contains("QUERYALL")) {
//							while(recovering){
//								Thread.yield();
//
//							}
							//lock.lock(); System.out.println("Locked at "+selfID);
							System.out.println("Received Query ALL at " + selfID);
//							System.out.println("Trying to split " + msgAr[0]);
//							String node = msgAr[0].split(":")[1];
							System.out.println("Check if key belongs to "+msgAr[1]);
							String node = msgAr[1];
							PrintWriter pw = new PrintWriter(s.getOutputStream(), true);

							String[] fileArray = getContext().getFilesDir().list();
							int count = fileArray.length;
							String test = Arrays.toString(fileArray);
							if (test.contains("Check")) {
								count = count - 1;
							}
							pw.println(count);
							System.out.println("Sent count for * as " + count + " from " + selfID);
							pw.flush();
							for (String file : fileArray) {
								try {
//                        FileInputStream fis = getContext().openFileInput(file);
//                        if(fis!=null){
									if (file.equals("Check")) {
										continue;
									}
									String keyHash = null;
									try {
										keyHash = genHash(file);
									} catch (NoSuchAlgorithmException e) {
										e.printStackTrace();
									}

									String pos = getPos(keyHash);
									String avd = nodeHash.get(pos);
									System.out.println("Key AVD is "+avd);
									if(avd.equals(node)) {
//                        }
										BufferedReader br2 = new BufferedReader(new InputStreamReader(getContext().openFileInput(file)));
										String res = br2.readLine();
//                                    if ((res = br2.readLine()) != null) {
//                                        cursor.addRow(new Object[]{file, res});
										System.out.println("Sending value for * " + file + "  " + res);
										pw.println(file + ":" + res);
										pw.flush();
									}


								} catch (FileNotFoundException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}

							}
							pw.flush();
							pw.close();

//                            pw.println(response);

							//lock.unlock(); System.out.println("Unlocked at "+selfID);
						}

						if (msgAr[0].equals("DELETEALL")) {
//							lock.lock(); System.out.println("Locked at "+selfID);

							String[] fileArray = getContext().getFilesDir().list();
							for (String file : fileArray) {
//                    try {
								if (file.equals("Check")) {
									continue;
								}
								System.out.println("Deleting "+file+" at " + selfID);
								getContext().deleteFile(file);
							}
							//lock.unlock(); System.out.println("Unlocked at "+selfID);

						}
						if (msgAr[0].equals("DELETEREDIR")) {
							//lock.lock(); System.out.println("Locked at "+selfID);
							System.out.println("REDIR Deleting " + msgAr[1] + " at " + selfID);

							String key = msgAr[1];
							getContext().deleteFile(key);
							//lock.unlock(); System.out.println("Unlocked at "+selfID);

						}


					}
//                    if (ois.readObject()!=null){
//                    Msg received = (Msg) ois.readObject();
//                        if(received.type.equals("TABLE")){
//                            nodeHash = received.table;
//                            System.out.println("Received TABLE at " + selfID);
//                            System.out.println("Table is " + nodeHash);
//                        }
//
//
//                    }

				} catch (IOException e) {
					e.printStackTrace();
//                        System.out.println("Socket ERROR at port " + s.getPort());
//                        portList.remove(s.getPort());
//                        deadPort = "deadport:"+s.getPort()+":";
//                    Log.e(TAG, "ServerSocket Exception");
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				finally {
//					lock.unlock();
				}
//             }
			}
//}
		}


	}


	private class ClientTask extends AsyncTask<String, Void, MatrixCursor> {
		//https://developer.android.com/reference/android/os/AsyncTask.html
// Changed return type of ClientTask to MatrixCursor so that we return a matrix cursor upon querying
		@Override
//        protected Void doInBackground(String... msgs) {
		protected MatrixCursor doInBackground(String... msgs) {

			String msgRec = msgs[0];
//

			if(msgRec.contains("RECOVER")){
//				lock.lock();
//				lock1.lock();
				recovering=true;
				getMsgs();



			}
//
			else if (msgRec.contains("INSERT")) {
//				lock.lock();
				try {

					String msgAr[] = msgs[1].split(":");
					Boolean success = false;
					int tries = 0;
					while (!success && (tries < 5)) {

						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									(Integer.parseInt(msgAr[2]) * 2));
							String msgToSend = "INSERT" + ":" + msgAr[0] + ":" + msgAr[1];
							System.out.println("Sending " + msgToSend + " to " + msgAr[2]);
							socket.setSoTimeout(3000);
							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							pw.println(msgToSend);
//							Thread.sleep(50);
							InputStreamReader is = new InputStreamReader(socket.getInputStream());
							BufferedReader br1 = new BufferedReader(is);
							String ack;
							if ((ack = br1.readLine()) != null) {
								System.out.println("Received ACK for INSERT from " + msgAr[2] + " for key " + msgAr[0]);
								if (ack.equals("Received Insert")) {
									success = true;
//									inserting = false;

									break;
								} else {
									System.out.println("Insert ACK Failed 2");
									continue;
								}
							} else {
								if (socket.isConnected()) {
									System.out.println("Socket is connected");
									System.out.println("Insert ACK Failed 1");
									throw new SocketTimeoutException();
								} else {
									System.out.println("Socket not connected");
									break;
								}
//							continue;
							}
//

						} catch (SocketTimeoutException e) {
							System.out.println("Socket timeout at insert for port " + msgAr[2]);
							tries++;
//						break;

						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
//						} catch (InterruptedException e) {
//							e.printStackTrace();
						} finally {
//					lock.unlock();

						}
					}
					if (!success) {
						//INSERT send to co-ordinator FAILED. Now sending to it's successors

						String[] succ = succList.get(msgAr[2]).split(":");
						String suc1 = succ[0];
						String suc2 = succ[1];
						boolean try1 = false, try2 = false;
						int tries1 = 0, tries2 = 0;
						while (!try1 && (tries1 < 5)) {
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										(Integer.parseInt(suc1) * 2));
								socket.setSoTimeout(3000);
								String msgToSend = "REPLICATE" + ":" + msgAr[0] + ":" + msgAr[1];
								System.out.println("Sending " + msgToSend + " to " + suc1);


								PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
								pw.println(msgToSend);
//								Thread.sleep(50);
								InputStreamReader is = new InputStreamReader(socket.getInputStream());
								BufferedReader br1 = new BufferedReader(is);
								String ack;
								if ((ack = br1.readLine()) != null) {
									System.out.println("Received ACK for replicate from " + suc1 + " for key " + msgAr[0]);
									if (ack.equals("Received Replicate")) {
										try1 = true;
										break;
									} else {
										System.out.println("Replicate ACK Failed 2");
										continue;
									}
								} else {
									if (socket.isConnected()) {
										System.out.println("Socket is connected");
										System.out.println("Replicate ACK Failed 1");
										throw new SocketTimeoutException();

									} else {
										System.out.println("Socket not connected");
										break;
									}

//							continue;
								}

//                    pw.flush();
//                    pw.println(nodeHash.size());
//                    pw.flush();
//                    for (String s : nodeHash.keySet()) {
//                        String msg = s + ":" + nodeHash.get(s);
//                        pw.println(msg);
//                        pw.flush();
//                    }


							} catch (SocketTimeoutException e) {
								System.out.println("Socket timeout at replicate for port " + suc1);
								tries1++;
//						break;
//						continue;
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
//							} catch (InterruptedException e) {
//								e.printStackTrace();
							} finally {
//					 lock.unlock();

							}
						}
						while (!try2 && (tries2 < 5)) {
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										(Integer.parseInt(suc2) * 2));
								socket.setSoTimeout(3000);
								String msgToSend = "REPLICATE" + ":" + msgAr[0] + ":" + msgAr[1];
								System.out.println("Sending " + msgToSend + " to " + suc2);


								PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
								pw.println(msgToSend);
//								Thread.sleep(50);
								InputStreamReader is = new InputStreamReader(socket.getInputStream());
								BufferedReader br1 = new BufferedReader(is);
								String ack;
								if ((ack = br1.readLine()) != null) {
									System.out.println("Received ACK for replicate from " + suc2 + " for key " + msgAr[0]);
									if (ack.equals("Received Replicate")) {
										try2 = true;
//										inserting= false;
										break;
									} else {
										System.out.println("Replicate ACK Failed 2");
										continue;
									}
								} else {
									if (socket.isConnected()) {
										System.out.println("Socket is connected");
										System.out.println("Replicate ACK Failed 1");
										throw new SocketTimeoutException();

									} else {
										System.out.println("Socket not connected");
										break;
									}

//							continue;
								}

//                    pw.flush();
//                    pw.println(nodeHash.size());
//                    pw.flush();
//                    for (String s : nodeHash.keySet()) {
//                        String msg = s + ":" + nodeHash.get(s);
//                        pw.println(msg);
//                        pw.flush();
//                    }


							} catch (SocketTimeoutException e) {
								System.out.println("Socket timeout at replicate for port " + suc2);
								tries2++;
//						break;
//						continue;
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
//							} catch (InterruptedException e) {
//								e.printStackTrace();
							} finally {
//					 lock.unlock();
//
//								inserting = false;
							}
						}


					}
				}finally{
//					inserting = false;
				}

			}
			else if (msgRec.contains("REPLICATE")) {
//				lock.lock();
				String msgAr[] = msgs[1].split(":");
				Boolean success = false;
				int tries = 0;
				while(!success && (tries<5)) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								(Integer.parseInt(msgAr[2]) * 2));
						socket.setSoTimeout(3000);
						String msgToSend = "REPLICATE" + ":" + msgAr[0] + ":" + msgAr[1];
						System.out.println("Sending " + msgToSend + " to " + msgAr[2]);


						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(msgToSend);
//						Thread.sleep(50);
						InputStreamReader is = new InputStreamReader(socket.getInputStream());
						BufferedReader br1 = new BufferedReader(is);
						String ack;
						if ((ack = br1.readLine()) != null)
						{
							System.out.println("Received ACK for replicate from "+msgAr[2]+" for key "+msgAr[0]);
							if (ack.equals("Received Replicate")){
								success = true;
								break;
							}
							else{
								System.out.println("Replicate ACK Failed 2");
								continue;
							}
						}
						else{
							if(socket.isConnected()) {
								System.out.println("Socket is connected");
								System.out.println("Replicate ACK Failed 1");
								throw new SocketTimeoutException();

							}else{
								System.out.println("Socket not connected");
								break;
							}

//							continue;
						}

//                    pw.flush();
//                    pw.println(nodeHash.size());
//                    pw.flush();
//                    for (String s : nodeHash.keySet()) {
//                        String msg = s + ":" + nodeHash.get(s);
//                        pw.println(msg);
//                        pw.flush();
//                    }


					} catch (SocketTimeoutException e) {
						System.out.println("Socket timeout at replicate for port "+msgAr[2]);
						tries++;
//						break;
//						continue;
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
					} finally {
//					 lock.unlock();

					}
				}

			}



			else if (msgRec.contains("QUERYREDIR")) {
//				lock.lock();

				String msgAr[] = msgs[1].split(":");

				try {
					System.out.println("Trying QUERYREDIR at "+selfID+" to port " + msgAr[2] +" for key " + msgAr[0]);
					Socket socket = null;
//                    do {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							(Integer.parseInt(msgAr[2]) * 2));
//                    } while(!socket.isConnected());
					socket.setSoTimeout(5000);
					System.out.println("Socket connected");
					String rep;

					String msgToSend = "QUERY" + ":" + msgAr[0] + ":" + msgAr[1];

					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(msgToSend);
//                    pw.flush();
					System.out.println("Sent QUERYREDIR");
//                    pw.close();
//					boolean success = false;
					Thread.sleep(100);
					MatrixCursor res = new MatrixCursor(new String[]{"key","value"});
					InputStreamReader is = new InputStreamReader(socket.getInputStream());
					BufferedReader br1 = new BufferedReader(is);
					if ((rep = br1.readLine()) != null)
					{
//						success = true;
						System.out.println("Response received " + rep);
						System.out.println("Inside readline Loop with "+ rep);
						String[] resp = rep.split(":");
						String out = resp[1];

						res.addRow(new Object[]{msgAr[0], out});
					}
					else{
						throw new SocketTimeoutException();
					}


//					lock.unlock();
					return res;


				}catch (SocketTimeoutException e) {
					System.out.println("Socket failure at port " + msgAr[2]);
//					String backup = succList.get(msgAr[2]);
//					String[] back = backup.split(":");
//					String back1 = back[0];
					String backup = predList.get(msgAr[2]);
					String[] back = backup.split(":");
					String back1 = back[0];
					System.out.println("Trying queryredir with backup port " + back1);
					if (back1.equals(selfID)) {
						BufferedReader br = null;
						try {
							br = new BufferedReader(new InputStreamReader(getContext().openFileInput(msgAr[0])));
						} catch (FileNotFoundException e1) {
							e1.printStackTrace();
						}
						String res;
						try {
							if ((res = br.readLine()) != null) {
								MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});

								cursor.addRow(new Object[]{msgAr[0], res});
//								lock.unlock();
								return cursor;
							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}

					} else {
						Socket socket = null;
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									(Integer.parseInt(back1) * 2));

							String rep1;

							String msgToSend1 = "QUERY" + ":" + msgAr[0] + ":" + msgAr[1];

							PrintWriter pw1 = null;
							try {
								pw1 = new PrintWriter(socket.getOutputStream(), true);
							} catch (IOException e1) {
								e1.printStackTrace();
							}
							pw1.println(msgToSend1);
//                    pw.flush();
//						System.out.println("Sent QUERYREDIR");
//                    pw.close();
							Thread.sleep(100);
//						MatrixCursor res = new MatrixCursor(new String[]{"key","value"});
							InputStreamReader is1 = null;
							is1 = new InputStreamReader(socket.getInputStream());
							MatrixCursor res = new MatrixCursor(new String[]{"key", "value"});

							BufferedReader br2 = new BufferedReader(is1);
							if ((rep1 = br2.readLine()) != null) {
								System.out.println("Response received " + rep1);
								System.out.println("Inside readline Loop with " + rep1);
								String[] resp = rep1.split(":");
								String out = resp[1];


								res.addRow(new Object[]{msgAr[0], out});
							}
//							lock.unlock();
							return res;


						} catch (UnknownHostException e1) {
							e1.printStackTrace();
						} catch (IOException e1) {
							e1.printStackTrace();
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				}
				catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				finally {
//					lock.unlock();
				}
//				;


			}
			else if (msgRec.contains("QUERYALL")) {
//				 lock.lock(); System.out.println("Locked at "+selfID);

				//http://stackoverflow.com/questions/5757202/how-would-i-print-all-values-in-a-treemap
				MatrixCursor res = new MatrixCursor(new String[]{"key","value"});

				for(Map.Entry<String,String> entry: nodeHash.entrySet()) {
					String node = entry.getValue();
					System.out.println("Checking if "+node+" equals "+selfID);
//					if (node.equals(selfID)) {
					if (node.equals(pred2)) {
						System.out.println("YES");
						System.out.println("Entering local @ query at " + selfID);
						String[] fileArray = getContext().getFilesDir().list();
						for (String file : fileArray) {
							String keyHash = null;
							try {
								keyHash = genHash(file);
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}

							String pos = getPos(keyHash);
							String avd = nodeHash.get(pos);
							if(avd.equals(pred2)) {
								try {
//                        FileInputStream fis = getContext().openFileInput(file);
//                        if(fis!=null){
//
//                        }
									BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(file)));
									String res1;
									if ((res1 = br.readLine()) != null) {
										res.addRow(new Object[]{file, res1});
									}
								} catch (FileNotFoundException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}

					}

					else{








							String nodeSucc1 = succList.get(node).split(":")[0];
							String nodeSucc2 = succList.get(node).split(":")[1];

						try {
							System.out.println("Trying QUERYALL at "+selfID+" to port " + nodeSucc2 +" for * ");
							Socket socket = null;
//                    do {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									(Integer.parseInt(nodeSucc2) * 2));
//                    } while(!socket.isConnected());
							socket.setSoTimeout(3000);
							System.out.println("Socket connected");
							String rep;

							String msgToSend = "QUERYALL:"+node+":";
							System.out.println("Sent req for QUERYALL "+msgToSend);

							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							pw.println(msgToSend);
//                    pw.flush();
							System.out.println("Sent QUERYALL");
//                    pw.close();
//							Thread.sleep(50);
//                            MatrixCursor res = new MatrixCursor(new String[]{"key","value"});
							InputStreamReader is = new InputStreamReader(socket.getInputStream());
							BufferedReader br1 = new BufferedReader(is);
							String rep1;
							if ((rep1 = br1.readLine()) != null) {

								int count = Integer.parseInt(rep1);
								System.out.println("Received count for QUERYALL " + count);
//                            if ((rep = br1.readLine()) != null)
//                            {
								String line;
//                                int count = Integer.parseInt(rep);
//								for (int i = 0; i < count; i++) {
								while((line=br1.readLine())!=null){
//									line = br1.readLine();

									String[] tab = line.split(":");
									System.out.println("Adding row to res for " + node + " key is " + tab[0] + " value is " + tab[1]);
									res.addRow(new Object[]{tab[0], tab[1]});
								}
							}
							else{
								throw new SocketTimeoutException();
//								System.out.println("Trying QUERYALL at "+selfID+" to port " + nodeSucc1 +" for * ");
//
//								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//										(Integer.parseInt(nodeSucc1) * 2));
////                    } while(!socket.isConnected());
//								socket.setSoTimeout(3000);
//								System.out.println("Socket connected");
//								String rep2;
//
//								String msgToSend1 = "QUERYALL:"+node+":";
//								System.out.println("Sent req for QUERYALL "+msgToSend1);
//
//								PrintWriter pw1 = new PrintWriter(socket.getOutputStream(), true);
//								pw.println(msgToSend1);
////                    pw.flush();
//								System.out.println("Sent QUERYALL");
////                    pw.close();
////								Thread.sleep(50);
////                            MatrixCursor res = new MatrixCursor(new String[]{"key","value"});
//								InputStreamReader is1 = new InputStreamReader(socket.getInputStream());
//								BufferedReader br2 = new BufferedReader(is1);
//								String rep3;
//								if ((rep3 = br2.readLine()) != null) {
//
//									int count = Integer.parseInt(rep3);
//									System.out.println("Received count for QUERYALL " + count);
////                            if ((rep = br1.readLine()) != null)
////                            {
//									String line;
////                                int count = Integer.parseInt(rep);
////								for (int i = 0; i < count; i++) {
//									while((line=br2.readLine())!=null){
////										line = br2.readLine();
//
//										String[] tab = line.split(":");
//										System.out.println("Adding row to res for " + node + " key is " + tab[0] + " value is " + tab[1]);
//										res.addRow(new Object[]{tab[0], tab[1]});
//									}
//								}
////								else{
////									continue;
////								}
//
//
//
//
////								continue;
							}
//                                System.out.println("Response received " + rep);
//                                System.out.println("Inside readline Loop with "+ rep);
//                                String[] resp = rep.split(":");
//                                String out = resp[1];

//                                res.addRow(new Object[]{msgAr[0], out});
//                            }
//                            return res;


						}catch (SocketTimeoutException e) {
							System.out.println("Trying QUERYALL at " + selfID + " to port " + nodeSucc1 + " for * ");

							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										(Integer.parseInt(nodeSucc1) * 2));

//                    } while(!socket.isConnected());
								socket.setSoTimeout(3000);
								System.out.println("Socket connected");
								String rep2;

								String msgToSend1 = "QUERYALL:" + node + ":";
								System.out.println("Sent req for QUERYALL " + msgToSend1);

								PrintWriter pw1 = new PrintWriter(socket.getOutputStream(), true);
								pw1.println(msgToSend1);
//                    pw.flush();
								System.out.println("Sent QUERYALL");
//                    pw.close();
//								try {
//									Thread.sleep(50);
//								} catch (InterruptedException e1) {
//									e1.printStackTrace();
//								}
//                            MatrixCursor res = new MatrixCursor(new String[]{"key","value"});
								InputStreamReader is1 = new InputStreamReader(socket.getInputStream());
								BufferedReader br2 = new BufferedReader(is1);
								String rep3;
								if ((rep3 = br2.readLine()) != null) {

									int count = Integer.parseInt(rep3);
									System.out.println("Received count for QUERYALL " + count);
//                            if ((rep = br1.readLine()) != null)
//                            {
									String line;
//                                int count = Integer.parseInt(rep);
//								for (int i = 0; i < count; i++) {
									while ((line = br2.readLine()) != null) {
//										line = br2.readLine();

										String[] tab = line.split(":");
										System.out.println("Adding row to res for " + node + " key is " + tab[0] + " value is " + tab[1]);
										res.addRow(new Object[]{tab[0], tab[1]});
									}
								}

							} catch (UnknownHostException e2) {
								e.printStackTrace();
							} catch (IOException e2) {
								e.printStackTrace();
							} finally {
//							lock.unlock();
							}
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (SocketException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}


					}


				}
//				 lock.unlock(); System.out.println("Unlocked at "+selfID);
				return res;

			}else if (msgRec.contains("DELETEALL")) {



				for(Map.Entry<String,String> entry: nodeHash.entrySet()) {
					String node = entry.getValue();
					if (node.equals(selfID)) {
						String[] fileArray = getContext().getFilesDir().list();
						for (String file : fileArray) {
//                    try {
							if(file.equals("Check")){
								continue;
							}
							getContext().deleteFile(file);
						}
					}
					else{

						Socket socket = null;
//                    do {
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									(Integer.parseInt(node) * 2));
							String msgToSend = "DELETEALL";

							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							pw.println(msgToSend);
//							Thread.sleep(100);



						} catch (IOException e) {
							e.printStackTrace();
						}


					}

				}


			}
			else if (msgRec.contains("DELETEREDIR")) {
//				lock.lock();

				String msgAr[] = msgs[1].split(":");


				Socket socket = null;
//                    do {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							(Integer.parseInt(msgAr[1]) * 2));
					String msgToSend = "DELETEREDIR"+":"+msgAr[0];

					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(msgToSend);
//					Thread.sleep(100);

				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

//				lock.unlock();

			}

			return null;

		}

		private void getMsgs(){
			try {

				String rep1 = recoverNew(pred1);
//				String rep2 = recoverNew(pred1);
//				String rep3 = recoverNew(succ1);
				String rep4 = recoverNew(succ1);



				if(rep1!= null && !rep1.equals("")){

					String[] files = rep1.split(":");
					if((files.length)>2) {
						StringBuilder strBuilder = new StringBuilder();
						for (int i = 0; i < files.length; i++) {
							strBuilder.append(files[i]);
						}
						System.out.println("Received recovery reply is "+strBuilder.toString());
						System.out.println("Length of response is "+files.length);
						for (int i = 1; i < files.length; i=i+2) {
							System.out.println("Current i is "+i);
							FileOutputStream outputStream;
							String key = files[i];
							String val = files[i + 1];
							String keyHash = genHash(key);
							String pos = getPos(keyHash);
							String avd = nodeHash.get(pos);
							System.out.println("Query AVD is " + avd + " selfID is " + selfID);
//							if ((avd.equals(selfID)) || avd.equals(pred1) || avd.equals(pred2)) {
							if(avd.equals(pred1)||avd.equals(pred2)){
//							if(avd.equals(pred2)){

								try {
									outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
									outputStream.write(val.getBytes());
									outputStream.close();
									Log.v("insert", key + " : " + val);
								} catch (Exception e) {
									//  Log.e(TAG, "File write failed");
								}
							}
						}
					}
				}

				if(rep4!= null && !rep4.equals("")){

					String[] files = rep4.split(":");
					if((files.length)>2) {
						System.out.println("Length of response is "+files.length);
						for (int i = 1; i < files.length; i = i + 2) {
							FileOutputStream outputStream;
							String key = files[i];
							String val = files[i + 1];
							String keyHash = genHash(key);
							String pos = getPos(keyHash);
							String avd = nodeHash.get(pos);
							System.out.println("Query AVD is " + avd + " selfID is " + selfID);
//							if ((avd.equals(selfID)) || avd.equals(pred1) || avd.equals(pred2)) {
							if((avd.equals(selfID))){

								try {
									outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
									outputStream.write(val.getBytes());
									outputStream.close();
									Log.v("insert", key + " : " + val);
								} catch (Exception e) {
									//  Log.e(TAG, "File write failed");
								}
							}
						}
					}
				}
				recovering=false;



			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} finally {

//					lock.unlock();
//					lock1.unlock();
			}
		}


	}


}
