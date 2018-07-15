import java.net.*;
import java.io.*;
import java.util.*;
import java.text.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

class Packet {
	public String message;
	int min;
	int sec;
	long minL;
	long secL;
	long totalMS;

	public Packet(int min, int sec, String message) {
		this.min = min;
		this.sec = sec;
		this.message = message;
		this.minL = min * 60000;
		this.secL = sec * 1000;
		this.totalMS = minL + secL;
	}
}

class Clock {
	static long clock;
	Clock() {
		clock = System.currentTimeMillis();
	}
}

class Token {
 	static int token;
 		Token() {
 			token = new Random().nextInt(10000);
 		
 		}
}

class P2PBulletin {
	
	long lmsgt = 0;
	long rtt = 0;

	volatile public static int lowp = 0;
	volatile public static int higp = 0;
	volatile public static int mport = 0;
	volatile public static int nhop = 0;
	volatile public static int phop = 0;
	volatile int fwdm = 0;
  	volatile int fwdo = 0;
  	volatile int elecl = 0;
  	volatile int ogsnd = 0;
  	volatile Boolean probing = true;
  	volatile Boolean electing = false;
  	volatile Boolean select = false;
  	volatile Boolean pmsg = false;
  	volatile Boolean chkr = false;


	public static DatagramSocket socket = null;
	public static long clock = System.currentTimeMillis();
	public static long join_time;
	public static long leave_time;

	String status1 = "HOP_REQUEST";
  	String status2 = "HOP_REPLY";
  	String status3 = "ELECT";
  	String status4 = "ELECTED";
  	String status5 = "POST";
  	String status6 = "TOKEN";
  	String status7 = "NEW";
  	String tokenID;
  	String previousToken;
  	String fwdp = "";
  	
  	Boolean ksnd = true;
  	Boolean htk = false;

  	
  	static Vector<Packet> inpmsg = new Vector<Packet>();
  	static String outputFile;

  	public static final String NEXTOUT = "next hop is changed to client ";
  	public static final String LASTOUT = "previous hop is changed to client ";
  	public static final String ELECTOUT = "started election, send election message to client ";
  	public static final String IAMLEADING = "relayed election message, replaced leader";
  	public static final String OTHERLEAD = "relayed election message, leader client ";
  	public static final String SELECTLEAD = "leader selected ";
  	public static final String CREATETOKEN = "new token generated ";
  	public static final String ST1 = "token ";
  	public static final String ST2 = "was sent to client ";
  	public static final String RT1 = "token ";
  	public static final String RT2 = "was received";
  	public static final String RP1 = "post ";
  	public static final String RP2 = "was received";
  	public static final String SP1 = "post ";
  	public static final String SP2 = "was sent";
  	public static final String FP1 = "post ";
  	public static final String FP2 = "from client ";
  	public static final String FP3 = "was relayed";
  	public static final String SUP1 = "post ";
  	public static final String SUP2 = "was delivered to all successfully";
  	public static final String ERROR = "ring is broken";
  	InetAddress haddr = null;
	Runnable receive, send, check;
  	Thread t1, t2, t3;

	public void createRing(int lowp, int higp, int mport) {
		try {
			haddr = InetAddress.getByName("localhost");
			socket = new DatagramSocket(mport);
			receive = new ReceiveThread();
			send = new SendThread();
			check = new CheckThread();
			t1 = new Thread(receive);
			t2 = new Thread(send);
			t3 = new Thread(check);				
			t1.start();
			t2.start();
			t3.start();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	String outp(String prepareThis) {
		String[] splitThis = prepareThis.split("_");
		String lastPart = splitThis[splitThis.length-1];
		return lastPart;
	}
	String formatdate(){
		SimpleDateFormat formattedDate = new SimpleDateFormat("mm:ss");
		Date date = new Date(System.currentTimeMillis()-clock);
      	String result = "[" + formattedDate.format(date) + "] ";
		return result;
	}

	public void dumpout(String writeThis) {
		try{
			BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile, true));
			outputWriter.write(formatdate() + outp(writeThis));
    		outputWriter.write("\n");
    		outputWriter.close();

		} 
		catch (IOException e) {
   			System.out.println(e.getMessage());
		}
	}

	class CheckThread implements Runnable {
		public void run() {
			nodedynamic();
		}

		public void nodedynamic() {
			try {
				while(!Thread.currentThread().isInterrupted()) {
					if (chkr){
					if (rtt != 0) {
						long temp=lmsgt + 2000;
						if (System.currentTimeMillis() > (lmsgt + 2000)) {
							dumpout(ERROR);	
							nhop = 0;
							phop = 0;
							fwdo = 0;
							fwdm = 0;
							elecl = 0;	
							ogsnd = 0;	
							lmsgt = System.currentTimeMillis();
							probing = true;
							electing = false;
							select = false;
							pmsg = false;
							chkr = false;
							htk = false;
							fwdp = "";
						}
						else {
							t3.sleep(1000);							
						}						
					}
					else {
						if (System.currentTimeMillis() > (lmsgt + 2000)) {
							dumpout(ERROR);
							nhop = 0;
							phop = 0;
							fwdo = 0;
							fwdm = 0;
							elecl = 0;
							ogsnd = 0;
							lmsgt = System.currentTimeMillis();
							probing = true;
							electing = false;
							select = false;
							pmsg = false;
							chkr = false;
							htk = false;
							fwdp = "";
						}
						else {
							t3.sleep(2000);							
						}
					}
				}}
			}
							
			catch(Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	class ReceiveThread implements Runnable {
		private SendThread sendthread;
		Boolean electmsg(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status3)) {
				return true;
			}
			return false;
		}

		Boolean electmsgme(String msg) {
			String[] packet_content = msg.split("_");
			if (Integer.parseInt(packet_content[1]) == mport) {
				return true;
			}
			return false;
		}

		Boolean leadelect(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status4)) {
				return true;
			}
			return false;
		}
		
		Boolean postmsg(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status5)) {
				return true;
			}
			return false;
		}
		
		Boolean tokenmsg(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status6)) {
				return true;
			}
			return false;
		}

		Boolean clnt(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status7)) {
				return true;
			}
			return false;
		}

		String toknm(String msg) {
			String[] arrivedToken = msg.split("_");
			if (arrivedToken[0].equals(status6)) {
				return arrivedToken[1];
			}
			return null;
		}

		String elctsnd(String msg) {
			String[] packet_content = msg.split("_");
			if ((packet_content[0]).equals(status3)) {
				return packet_content[1];
			}
			if (packet_content[0].equals(status4)) {
				return packet_content[1];
			}
			return null;
		}

		String elctmsg(String msg) {
			String[] packet_content = msg.split("_");
			if ((packet_content[0]).equals(status3)) {
				return packet_content[2];
			}
			if (packet_content[0].equals(status4)) {
				return packet_content[1];
			}
			return null;
		}

		String pid(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status5)) {
				return packet_content[1];
			}
			return null;
		}

		String gpstmsg(String msg) {
			String[] packet_content = msg.split("_");
			if (packet_content[0].equals(status5)) {
				return packet_content[2];
			}
			return null;
		}

		public void run() {
			receive();
		}
	
		public void receive() {
			try {
				byte[] sendReply = new byte[1024]; 
				byte[] receiveData = new byte[1024];
					try {
						while(true) {
							// Leave the ring if the time is over
							if (System.currentTimeMillis() >= leave_time) {
								System.exit(0);
							}
							DatagramPacket receivePacket = new DatagramPacket(receiveData, 1024);
							socket.receive(receivePacket);
							int receivedPort = receivePacket.getPort();
    						String packetMessage = new String(receivePacket.getData(), 0, receivePacket.getLength());

							if (packetMessage.equals(status1) && receivedPort >= phop) {
								phop = receivedPort;
    							sendReply = status2.getBytes();
    							DatagramPacket sendReplyPacket = new DatagramPacket(sendReply, sendReply.length, haddr, receivedPort);
   								socket.send(sendReplyPacket); 
   								
   							}

   							if (packetMessage.equals(status2)) {
								nhop = receivedPort;
								dumpout(NEXTOUT + "[port " + nhop +"]");
								
							}

							if (electmsg(packetMessage) && elecl == 0) {
								if(electmsgme(packetMessage) || (Integer.parseInt(elctsnd(packetMessage)) == mport)) {
									lmsgt = System.currentTimeMillis()+new Random().nextInt(1000);
								}

								String arrivedPort = elctmsg(packetMessage);
								ogsnd = Integer.parseInt(elctsnd(packetMessage));
								if (Integer.parseInt(arrivedPort) == mport) {
									lmsgt = System.currentTimeMillis();
									elecl = mport;
									select = true;
									dumpout(SELECTLEAD);
								}
					
								if (Integer.parseInt(arrivedPort) > mport && Integer.parseInt(arrivedPort) > fwdo) {
									fwdo = Integer.parseInt(arrivedPort);
									dumpout(OTHERLEAD + "[port " + Integer.toString(fwdo) + "]");
								}
								if (Integer.parseInt(arrivedPort) < mport) {
									fwdm = mport;
									dumpout(IAMLEADING);
 								}
							}

							if (leadelect(packetMessage)) {
								String newLeader = elctmsg(packetMessage);
								lmsgt = System.currentTimeMillis()+1000;
								elecl = Integer.parseInt(newLeader);
								electing = false;
								select = true;
								if (Integer.parseInt(newLeader) == mport) {
									Token token = new Token();
									tokenID = Integer.toString(Token.token);
									dumpout(CREATETOKEN + "[" + tokenID + "]");
									htk = true;
									electing = false;
									select = false;
									pmsg = true;
								}
							}
   					
							if (pmsg && phop == 0) {
	   							phop = receivedPort;
	   						}
	   						if (pmsg && packetMessage.equals(status1)) {
	   							phop = receivedPort;
	   						}
							if (postmsg(packetMessage)) {
								select = false;
								electing = false;
								if(Integer.parseInt(pid(packetMessage)) != mport) {
									pmsg = true;
									String newPost = gpstmsg(packetMessage);
									fwdp = packetMessage;
									lmsgt = System.currentTimeMillis();	
  									dumpout(RP1 + "\"" + outp(fwdp) + "\" " + RP2);
  									dumpout(FP1 + "\"" + outp(fwdp) + "\" " + FP2 + "[port " + Integer.toString(Integer.parseInt(pid(packetMessage))) + "] " + FP3);	
								}
								if ((Integer.parseInt(pid(packetMessage)) == mport) && receivedPort == phop) {
									rtt = System.currentTimeMillis() - lmsgt;
									lmsgt = System.currentTimeMillis();
									pmsg = true;	
									if(!inpmsg.isEmpty()) {									
										synchronized (inpmsg) { 
	    									for (Iterator<Packet> processPosts = inpmsg.iterator(); processPosts.hasNext(); ) {
												Packet processMessage = processPosts.next();
												if(gpstmsg(packetMessage).equals(processMessage.message)) {
													processPosts.remove();
													dumpout(SUP1 + "\"" + outp(packetMessage) + "\" " + SUP2);
												}
											}
										}
									}
								}	
		   					}								
					

							if (tokenmsg(packetMessage) && receivedPort == phop) {
								htk = true;
								tokenID = toknm(packetMessage);
								lmsgt = System.currentTimeMillis();									
								select = false;
								electing = false;									
								pmsg = true;
								fwdp = "";
								elecl = mport;
								if (previousToken == null || !previousToken.equals(tokenID)) {
									dumpout(RT1 + "[" + toknm(packetMessage) + "] " + RT2);
									previousToken = tokenID;
								}
							}
							
   					}}
    				catch(SocketTimeoutException e) {
    				}
				}
				catch(Exception e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
		}

	class SendThread implements Runnable {
		public void run() {
			send();
		}


		public void send() {
			try {
				while(true) {
					if (probing) {
						byte[] sendRequest = status1.getBytes();
						if (mport != higp) {
							for(int i = mport; i <= higp; i++) {
								if (nhop != 0){
									dumpout(NEXTOUT + "[port " + nhop + "]");
									dumpout(ELECTOUT + "[port " + nhop +"]");
									t2.sleep(new Random().nextInt(1000));
									probing = false;
									electing = true;
									lmsgt = System.currentTimeMillis();	
									break;
								}

								if (phop !=0 && nhop !=0){								
								  	dumpout(LASTOUT + "[port " + phop + "]");
									break;
								}

								if (mport != i) {
									DatagramPacket sendRequestPacket = new DatagramPacket(sendRequest, sendRequest.length, haddr, i);
									socket.send(sendRequestPacket);
								}
								if (i == higp){
									i = lowp-1;
								}


								t2.sleep(100);
		    				}
	    				}
	    				else {
							for(int i = lowp; i <= higp; i++) {
								if (nhop !=0){
									dumpout(NEXTOUT + "[port " + nhop + "]");
									t2.sleep(1000);
									probing = false;
									electing = true;
									lmsgt = System.currentTimeMillis();	
									break;									
								}
								if (phop !=0 && nhop !=0){
									lmsgt = System.currentTimeMillis();
								  	dumpout(LASTOUT + "[port " + phop + "]");	
								  	break;			
								}
								if (mport != i) {
									DatagramPacket sendRequestPacket = new DatagramPacket(sendRequest, sendRequest.length, haddr, i);
									socket.send(sendRequestPacket);	
									t2.sleep(100);
								}
								if (i == higp){
									i = lowp-1;
								}							
		    				}    					
	    				}
					} 
					if (electing) {
						chkr = true;
						if(fwdm == 0 && fwdo == 0 && elecl == 0) {
							String delimiter = status3+"_"+Integer.toString(mport)+"_";
							String electionString = delimiter + Integer.toString(mport);
							byte[] electionMessage = electionString.getBytes();
							DatagramPacket sendElectionPacket = new DatagramPacket(electionMessage, electionMessage.length, haddr, nhop);

							if (nhop != 0) {
								socket.send(sendElectionPacket);
							}
							t2.sleep(100);
						}
						if(fwdm != 0 && fwdo == 0 && elecl == 0) {
							String delimiter = status3+"_"+ Integer.toString(ogsnd)+"_";
							String electionString = delimiter + Integer.toString(fwdm);
							byte[] electionMessage = electionString.getBytes();
							DatagramPacket sendElectionPacket = new DatagramPacket(electionMessage, electionMessage.length, haddr, nhop);

							if (nhop != 0) {
								socket.send(sendElectionPacket);
							}
							t2.sleep(100);
						}
						if(fwdo != 0 && elecl == 0) {
							String delimiter = status3+"_"+ Integer.toString(ogsnd)+"_";
							String electionString = delimiter + Integer.toString(fwdo);
							byte[] electionMessage = electionString.getBytes();
							DatagramPacket sendElectionPacket = new DatagramPacket(electionMessage, electionMessage.length, haddr, nhop);

							if (nhop != 0) {
								socket.send(sendElectionPacket);
							}
							t2.sleep(100);
						}
						if(select) {
							electing = false;
							select = false;
							pmsg = true;
							String delimiter = status4+"_";
							String electionString = delimiter + Integer.toString(elecl);
							byte[] electionMessage = electionString.getBytes();
							DatagramPacket sendElectionPacket = new DatagramPacket(electionMessage, electionMessage.length, haddr, nhop);
							socket.send(sendElectionPacket);
							t2.sleep(100);
						}
					} 

					if (pmsg) {
						if (inpmsg.isEmpty() && htk) {
							rtt = 0;
							lmsgt = System.currentTimeMillis();
							String delimiter = status6+"_";
							String tokenString = delimiter + tokenID;
							byte[] tokenMessage = tokenString.getBytes();
							DatagramPacket sendTokenPacket = new DatagramPacket(tokenMessage, tokenMessage.length, haddr, nhop);
							socket.send(sendTokenPacket);
							htk = false;
							if (previousToken == null || !previousToken.equals(tokenID)) {
								dumpout(ST1 + "[" + tokenID + "] " + ST2 + "[port " + nhop + "]");
								previousToken = tokenID;
							}
							t2.sleep(100);
						}
						else if(htk) {
							lmsgt = System.currentTimeMillis();
							synchronized (inpmsg) { 
								Iterator<Packet> processPosts = inpmsg.iterator();
								while (processPosts.hasNext()) {
									Packet processMessage = processPosts.next();
									if(System.currentTimeMillis() - clock > processMessage.totalMS) {
																pmsg = false;

										String delimiter = status5+"_"+mport+"_";
										String postMsg = delimiter + processMessage.message;
										byte[] postMessage = postMsg.getBytes();
										DatagramPacket sendMessagePacket = new DatagramPacket(postMessage, postMessage.length, haddr, nhop);
										socket.send(sendMessagePacket);
										t2.sleep(100);
										}
									}
							}
							if (!inpmsg.isEmpty()) {
								rtt = 0;
								String delimiter = status6+"_";
								String tokenString = delimiter + tokenID;
								byte[] tokenMessage = tokenString.getBytes();
								DatagramPacket sendTokenPacket = new DatagramPacket(tokenMessage, tokenMessage.length, haddr, nhop);
								socket.send(sendTokenPacket);
								htk = false;
								if (previousToken == null || !previousToken.equals(tokenID)) {
									dumpout(ST1 + "[" + tokenID + "] " + ST2 + "[port " + nhop + "]");
									previousToken = tokenID;
								}
								t2.sleep(100);
							}
						}
						else if(!htk && !fwdp.isEmpty()) {
							String postMsg =  fwdp;
							byte[] postMessage = postMsg.getBytes();
							DatagramPacket sendMessagePacket = new DatagramPacket(postMessage, postMessage.length, haddr, nhop);
							socket.send(sendMessagePacket);
							fwdp = "";
							t2.sleep(100);
						}
					} 
				}
			}
			catch(Exception e) { 
				e.printStackTrace(); 
				System.out.println("Error1: " + e.getMessage());
			}
		}
	}



	


	public static void main(String args[]) {
		if(args.length == 6) 
		{
			if (args[0].equals("-c") && args[2].equals("-i") && args[4].equals("-o")) 
			{
				try 
				{
					Scanner con = new Scanner(args[1]);
					Scanner inp = new Scanner(args[3]);
					outputFile = args[5];

					//Reading config file
					File file1 = new File(con.nextLine());
            		con = new Scanner(file1);
					while (con.hasNextLine()) 
					{
						String line = con.nextLine();
						String[] tokens = line.split(" ");
						if (line.startsWith("client_port")) 
						{
							String[] ports = tokens[1].split("-");
							lowp = Integer.parseInt(ports[0]);
							higp = Integer.parseInt(ports[1]);
						}
						if (line.startsWith("my_port")) 
						{
							mport = Integer.parseInt(tokens[1]);
						}
						if (line.startsWith("join_time")) 
						{
							String[] time = tokens[1].split(":");
							long time1 = Integer.parseInt(time[0]) * 60000;
							long time2 = Integer.parseInt(time[1]) * 1000;
							join_time = System.currentTimeMillis() + (time1+time2);
						}
						if (line.startsWith("leave_time")) 
						{
							String[] time = tokens[1].split(":");
							long time1 = Integer.parseInt(time[0]) * 60000;
							long time2 = Integer.parseInt(time[1]) * 1000;
							leave_time = System.currentTimeMillis() + (time1+time2);
						}
					}
					con.close();	

					//Reading input file
					File file = new File(inp.nextLine());
            		inp = new Scanner(file);
            		while (inp.hasNextLine()) 
            		{
						String line = inp.nextLine();
						if (line.trim().length() > 0) 
						{
							String[] time = line.split(":");
							String[] tokens = time[1].split("\t");
							Packet obj = new Packet(Integer.parseInt(time[0]), Integer.parseInt(tokens[0]), tokens[1]);
							inpmsg.add(obj);
						}
					}
					inp.close();	
		
		
					long stime=join_time;
					if (System.currentTimeMillis() >= stime) 
					{
						P2PBulletin client = new P2PBulletin(); 
        				client.createRing(lowp, higp, mport);
					}
					else 
					{
        				while (stime > 0) 
        				{ 
            				if (stime == System.currentTimeMillis()) 
            				{
        						P2PBulletin client = new P2PBulletin(); 
        						client.createRing(lowp, higp, mport);
        						break;
            				}
        				}
    				}
				}

				catch (Exception e) {
					System.err.println("Error: " + e.getMessage());
	        	}
        	}
        }
        else {
	        System.out.println("insufficient arguments");
	        System.exit(0);
        }	
	}
}