package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private String myPortHash;
	private static String currentPort;

	static final String[] ports={"11108", "11112", "11116", "11120", "11124"};
	protected static ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList(ports));
	static final int SERVER_PORT = 10000;

	protected final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	final static int nodeRevive = 0;
    final static int insertOperation = 3;
    final static int queryOperation = 4;
    final static int deleteOperation = 5;
    final static int queryFromAllOperation = 6;
    final static int deleteFromAllOperation = 7;

	private DBHelper dbHelper;
	private static final String DBNAME = "SimpleDynamoDB";
	private SQLiteDatabase database;

	protected static final String KEY="key";
	protected static final String VALUE="value";

	static Map<String,String> hashToPort = new HashMap<String,String>();

	//static List<String> activePorts = new ArrayList<String>();
	static Map<String,Integer> isPortActive = new HashMap<String, Integer>();
	static List<String> dhtList = new ArrayList<String>();

	MessageBuffer messageBuffer=new MessageBuffer();

	private static String maxHash;
	private static String minHash;

    static int isFailureDetected=0;
    static String failedNode="none";



    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        currentPort=myPort;
        Log.e("My Code - Client", "My Port number : " + myPort);
        dbHelper = new DBHelper(context,DBNAME,null,1);
        messageBuffer.inactivePort="none";
        try {
            int emulatorNo=(Integer.parseInt(myPort))/2;
            Log.e("My Code - Client", "My Emulator number : " + emulatorNo);
            myPortHash=(genHash(String.valueOf(emulatorNo)));
            //Log.e("My Code - Client", "Hash value of my port string : " + getMyPortHash());

            for(int i=0;i<ports.length;i++){
                emulatorNo=(Integer.parseInt(ports[i]))/2;
                hashToPort.put(genHash(String.valueOf(emulatorNo)),ports[i]);
                //Log.e("My Code - Client", "Adding port : " + ports[i] + " to the hashmap with hash :" + genHash(ports[i]));
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        for(int i=0;i<ports.length;i++){
            isPortActive.put(ports[i],1);
        }
        establishDHT();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            //Log.e("My Code - Client", "Creating Server Thread");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new NodeRevive().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);

        }catch(UnknownHostException e) {
            Log.e("My Code - Client", "Can't create a client socket due to exception : " + e);
            return false;

        } catch(IOException e) {
            Log.e("My Code - Client", "Can't create a ServerSocket due to exception : " + e);
            return false;
        }

        return false;
    }

    private class NodeRevive extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            BufferedWriter bw=null;
            String myPortNo = msgs[0];

            Socket[] clientSocketArray = new Socket[5];
            Log.e("My code - NodeRevive", "Start of node join in client : "+myPortNo);
            for(int i=0;i<ports.length;i++){
                if(ports[i].equals(myPortNo))
                    continue;
                clientSocketArray[i] = new Socket();
                try {
                    Log.e("My code - NodeRevive", "Node Revive - Attempting to create client socket to port "+ports[i]);
                    clientSocketArray[i].connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports[i])));

                    OutputStream os = clientSocketArray[i].getOutputStream();
                    bw = new BufferedWriter(new OutputStreamWriter(os));

                    bw.write(String.valueOf(nodeRevive));
                    bw.newLine();
                    bw.flush();

                    bw.write(String.valueOf(myPortNo));
                    bw.newLine();
                    bw.flush();

                    InputStream is = clientSocketArray[i].getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));

                    int messageCount  = Integer.parseInt(br.readLine());
                    Log.e("My code - NodeRevive", "Message count in buffer = "+messageCount);

                    if(messageCount>0){
                        String key1;
                        String value1;

                        for(int j=0;j<messageCount;j++){
                            key1=br.readLine();
                            value1 = br.readLine();
                            ContentValues cv1 = new ContentValues();
                            cv1.put(KEY,key1);
                            cv1.put(VALUE,value1);
                            basicInsert(uri, cv1);
                            Log.e("My code - NodeRevive", "Basic insert number = " + (j+1));
                        }
                    }

                    bw.write("end");
                    bw.newLine();
                    bw.flush();

                } catch (UnknownHostException e) {
                    Log.e("My code - NodeRevive", "ClientTask UnknownHostException");
                    e.printStackTrace();
                }catch (SocketException e){
                    Log.e("My code - NodeRevive", "SocketException on port " +currentPort + " due to : " + e);
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e("My code - NodeRevive", "ClientTask socket IOException on port " + Integer.parseInt(myPortNo)*2);
                    e.printStackTrace();
                } catch (Exception e) {
                    Log.e("My code - NodeRevive", "Exception : "+e);
                    e.printStackTrace();
                }finally {
                    if(clientSocketArray[i]!=null){
                        try {
                            clientSocketArray[i].close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            Log.e("My code - NodeRevive", "End of node revive in client : "+myPortNo);
            return null;
        }

        public synchronized Uri basicInsert(Uri uri, ContentValues values)  {

            // TODO Auto-generated method stub
            //Log.e("My Code - Client", " Getting a writable instance of a db");
            SQLiteDatabase database = dbHelper.getWritableDatabase();

            //Log.e("My Code - Client", " Getting a writable instance of a db");

            long rowId=0;
            Uri retUri=null;
            try {
                rowId = database.replace(DBNAME, "", values);
                Log.e("My Code - Client", " Basic insert - Replacing element in db");
            }catch (Exception e) {
                Log.e("My Code - Client", "SQL write failed due to " + e);
                e.printStackTrace();
            }

            if (rowId > 0) {
                //Log.e("My Code - Client", "Basic insertion successful. Row "+rowId+" added to the table");
                getContext().getContentResolver().notifyChange(uri, null);
            }
            Log.v("insert", values.toString());
            return uri;
        }

    }

    private void establishDHT() {
        Log.e("My Code - Client", "Calling establishDHT");
        dhtList= new ArrayList<String>();
       /* if(!currentPort.equals("11108")){
            Log.e("My Code - Client", "establishDHT called from wrong process");
            return;
        }*/
        for(int i=0;i<ports.length;i++){
            //if(isPortActive.get(ports[i])==0)
                //continue;
            try {
                dhtList.add(genHash(String.valueOf(Integer.parseInt(ports[i])/2)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(dhtList);

        maxHash=dhtList.get(dhtList.size()-1);
        minHash=dhtList.get(0);

/*        Log.e("My Code - Client", "DHT Established with maxHash - "+maxHash+", i.e. port - "+hashToPort.get(maxHash)+" minHash - "+minHash+" i.e. port - "+hashToPort.get(minHash));
        for (int i=0;i<dhtList.size();i++){
            Log.e("My Code - Client", "Dht List element "+i+" : "+dhtList.get(i));
        }*/
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values)  {

        // TODO Auto-generated method stub
        Log.e("My Code - Client", "Insert at : "+currentPort);
        SQLiteDatabase database = dbHelper.getWritableDatabase();

        long rowId=0;
        Uri retUri=null;
        try {

            String hashKey = genHash(values.getAsString(KEY));
            Log.e("My Code - Client", "Insert - key : "+values.getAsString(KEY));

            String[] avdList = findAVD(hashKey);
/*            Log.e("My Code - Client", " Hashkey : "+hashKey);
        Log.e("My Code - Client", " Predecessor hash : "+predecessor);
        Log.e("My Code - Client", " My port hash : "+myPortHash);
        Log.e("My Code - Client", " Max hash : "+maxHash);
        Log.e("My Code - Client", " Min hash : "+minHash);*/

            for(int i=0;i<avdList.length;i++){
                if(avdList[i].equals(currentPort)){
                    //Log.e("My Code - Client", "Returning from insert ");
                    rowId = database.replace(DBNAME, "", values);
                }
                else if(isPortActive.get(avdList[i])==0){
                    Map<String,String> bufferMap =  messageBuffer.messageBufferMap;
                    bufferMap.put(values.getAsString(KEY), values.getAsString(VALUE));
                }
                else{
                    retUri = insertAt(avdList[i], values);
                }
            }
            //Log.e("My Code - Client", "Returning from insert ");
            return retUri;

        } catch (Exception e) {
            Log.e("My Code - Client", "SQL write failed due to " + e);
            e.printStackTrace();
        }

        if (rowId > 0) {
            //Log.e("My Code - Client", "Insertion successful. Row "+rowId+" added to the table");
            getContext().getContentResolver().notifyChange(uri, null);
        }

        Log.v("insert", values.toString());
        return uri;
    }

    private synchronized Uri insertAt(String id, ContentValues values) {
        BufferedWriter bw=null;
        Socket clientSocket=new Socket();
        Uri retUri=null;

        Log.e("My Code - Client", "insertAt at : "+currentPort+" to : "+id+" with key : "+String.valueOf(values.get(KEY)));
        try {
            clientSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id)));
            clientSocket.setSoTimeout(1500);

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            //Log.e("My Code - Client", "insertAt at - operation");
            bw.write(String.valueOf(insertOperation));
            bw.newLine();
            bw.flush();

            //Log.e("My Code - Client", "insertAt at - key");
            bw.write(String.valueOf(values.get(KEY)));
            bw.newLine();
            bw.flush();

            //Log.e("My Code - Client", "insertAt at - value");
            bw.write(String.valueOf(values.get(VALUE)));
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            //Log.e("My Code - Client", "insertAt at - read return Uri");
            retUri = Uri.parse(br.readLine());

            //Log.e("My Code - Client", "insertAt at - end");
            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My Code - Client", "ClientTask UnknownHostException");
        } catch (NumberFormatException e){
            handleFailure(id);
            messageBuffer.messageBufferMap.put(values.getAsString(KEY), values.getAsString(VALUE));
            Log.e("My code - Client", "NumberFormatException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (NullPointerException e) {
            handleFailure(id);
            messageBuffer.messageBufferMap.put(values.getAsString(KEY), values.getAsString(VALUE));
            Log.e("My code - Client", "SocketTimeoutException on port " + currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (SocketTimeoutException e){
            handleFailure(id);
            messageBuffer.messageBufferMap.put(values.getAsString(KEY), values.getAsString(VALUE));
            Log.e("My code - Client", "SocketTimeoutException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (SocketException e){
            handleFailure(id);
            messageBuffer.messageBufferMap.put(values.getAsString(KEY), values.getAsString(VALUE));
            Log.e("My code - Client", "SocketException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (EOFException e) {
            handleFailure(id);
            messageBuffer.messageBufferMap.put(values.getAsString(KEY), values.getAsString(VALUE));
            Log.e("My code - Client", "EOFException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (IOException e){
            Log.e("My code - Client", "IOException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (Exception e){
            Log.e("My code - Client", "Exception on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //Log.e("My Code - Client", "End of insertAt at : " + currentPort);
        return retUri;
    }

    private void handleFailure(String portId) {
        if(isFailureDetected==0){
            isFailureDetected=1;
            failedNode=portId;
            isPortActive.put(portId,0);
            messageBuffer.inactivePort=failedNode;
            Log.e("My code - Client", "Failure detected at "+portId+ ". isActive is set to 0");
        }
    }

    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        SQLiteDatabase database = dbHelper.getWritableDatabase();
        int rowsDeleted=0;
        try {
           // Log.e("My Code - Client", "Inside the try block for delete");
            String hashElement = genHash(selection);
            String[] args = new String[1];
            args[0] = selection;

            if(selection.equals("@")){
                Log.e("My Code - Client", "Deleting for @");
                rowsDeleted = database.delete(DBNAME, null, args);

            } else if (selection.equals("*")) {
                Log.e("My Code - Client", "Deleting for *");
                rowsDeleted = database.delete(DBNAME, null, args);

                for(int i=0;i<dhtList.size();i++){
                    if(hashToPort.get(dhtList.get(i)).equals(currentPort))
                        continue;
                    if(isPortActive.get(hashToPort.get(dhtList.get(i)))==0){
                        if(messageBuffer.inactivePort.equals(hashToPort.get(dhtList.get(i)))){
                            messageBuffer.messageBufferMap.remove(selection);
                        }
                    }
                    int rowsDeletedFromDHT = deleteFrom(hashToPort.get(dhtList.get(i)),"@");
                    rowsDeleted+=rowsDeletedFromDHT;
                }
            }
            else{

                String[] avdList = findAVD(hashElement);
                Log.e("My Code - Client", "Deleting for "+selection);
                for(int i=0;i<avdList.length;i++){
                    if(avdList[i].equals(currentPort))
                        rowsDeleted+= database.delete(DBNAME, "key =?", args);
                    else if(isPortActive.get(avdList[i])==0){
                        if(messageBuffer.inactivePort.equals(avdList[i])){
                            messageBuffer.messageBufferMap.remove(selection);
                        }
                    }
                    else {
                        rowsDeleted+= deleteFrom(avdList[i], selection);
                    }
                }
            }

        }catch (Exception e){
            Log.e("My Code - Client", "SQL delete failed due to " + e);
            e.printStackTrace();
        }

        Log.e("My Code - Client", "Number of rows deleted " + rowsDeleted);
        return rowsDeleted;
    }

    private synchronized int deleteFrom(String id, String selection) {
        BufferedWriter bw=null;
        Socket clientSocket=new Socket();
        int returnRows=0;

        Log.e("My Code - Client", "deleteFrom at : " + currentPort + " to : " + id+" with : "+selection);
        try {
            clientSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id)));
            clientSocket.setSoTimeout(1500);

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(deleteOperation));
            bw.newLine();
            bw.flush();

            bw.write(selection);
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            returnRows=Integer.parseInt(br.readLine());

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My Code - Client", "ClientTask UnknownHostException");
        } catch (NumberFormatException e){
            handleFailure(id);
            Log.e("My code - Client", "NumberFormatException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (NullPointerException e) {
            handleFailure(id);
            Log.e("My code - Client", "SocketTimeoutException on port " + currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (SocketTimeoutException e){
            handleFailure(id);
            Log.e("My code - Client", "SocketTimeoutException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (SocketException e){
            handleFailure(id);
            Log.e("My code - Client", "SocketException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (EOFException e) {
            handleFailure(id);
            Log.e("My code - Client", "EOFException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (IOException e){
            Log.e("My code - Client", "IOException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (Exception e){
            Log.e("My code - Client", "Exception on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //Log.e("My Code - Client", "End of deleteFrom at : " + currentPort);
        return returnRows;
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        SQLiteDatabase database = dbHelper.getReadableDatabase();

        Cursor c1=null;

        try{

            String hashElement = genHash(selection);
            String[] args = new String[1];
            args[0] = selection;

            if(selection.equals("@")){
                Log.e("My Code - Client", "Querying for @ ");
                c1 = database.query(
                        DBNAME,  // The table to query
                        null,                               // The columns to return
                        null,                                    // The columns for the WHERE clause
                        null,                                       // The values for the WHERE clause
                        null,                                     // don't group the rows
                        null,                                     // don't filter by row groups
                        null                                 // The sort order
                );
            }
            else if(selection.equals("*")){
                Log.e("My Code - Client", "Querying for * ");
                Cursor c0=database.query(
                        DBNAME,  // The table to query
                        null,                               // The columns to return
                        null,                                    // The columns for the WHERE clause
                        null,                                       // The values for the WHERE clause
                        null,                                     // don't group the rows
                        null,                                     // don't filter by row groups
                        null                                 // The sort order
                );

                int currentIndex=0;
                for(int i=0;i<dhtList.size();i++){
                    if(currentPort.equals(hashToPort.get(dhtList.get(i)))){
                        currentIndex=i;
                    }
                }
                Cursor c2 = new MergeCursor(new Cursor[]{c0});
                for(int i=0;i<dhtList.size()-1;i++) {
                        if(isPortActive.get(hashToPort.get(dhtList.get((currentIndex + i + 1) % 5)))==0)
                            continue;
                        Cursor c3 = queryFromAll(hashToPort.get(dhtList.get((currentIndex + i + 1) % 5)), "@");
                        c2 = new MergeCursor(new Cursor[]{c2, c3});
                    }
                c1 = c2;
            }
            else{
                Log.e("My Code - Client", "Querying for "+selection);
                String[] avdList = findAVD(hashElement);

                if (avdList[2].equals(currentPort)){
                    c1 = database.query(
                            DBNAME,  // The table to query
                            projection,                               // The columns to return
                            "key =?",                                    // The columns for the WHERE clause
                            args,                                       // The values for the WHERE clause
                            null,                                     // don't group the rows
                            null,                                     // don't filter by row groups
                            null                                 // The sort order
                    );
                }
                else{
                    if(isPortActive.get(avdList[2])!=0){
                        c1 = queryFrom(avdList[2], selection);
                        if(c1==null){
                            c1 = queryFrom(avdList[1], selection);
                        }
                        else{
                            c1.moveToFirst();
                            if(c1.getString(0)==null || c1.getString(1)==null){
                                c1 = queryFrom(avdList[1], selection);
                            }
                        }
                    }
                    else
                        c1 = queryFrom(avdList[1], selection);
                }
            }
        if (c1.moveToFirst())
                Log.e("My Code - Client", "The column contents for key : " + c1.getString(0) + " value : " + c1.getString(1));
        } catch (Exception e) {
            Log.e("My Code - Client", "SQL query failed due to " + e);
            e.printStackTrace();
        }

        Log.v("query", selection);
        return c1;
    }

    private synchronized Cursor queryFrom(String id, String selection) {
        BufferedWriter bw=null;
        Socket clientSocket=new Socket();
        MatrixCursor matrixCursor=null;

        Log.e("My Code - Client", "queryFrom at : " + currentPort+" to : "+id+" with key : "+selection);
        try {
            clientSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id)));
            clientSocket.setSoTimeout(1500);

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(queryOperation));
            bw.newLine();
            bw.flush();

            bw.write(selection);
            bw.newLine();
            bw.flush();


            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String receivedKey=br.readLine();
            String receivedValue=br.readLine();

            String[] columns = new String[] { "key", "value"};
            matrixCursor= new MatrixCursor(columns);
            matrixCursor.addRow(new String[] { receivedKey, receivedValue });

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My Code - Client", "ClientTask UnknownHostException");
        } catch (NumberFormatException e){
            handleFailure(id);
            Log.e("My code - Client", "NumberFormatException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (NullPointerException e) {
            handleFailure(id);
            Log.e("My code - Client", "SocketTimeoutException on port " + currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (SocketTimeoutException e){
            handleFailure(id);
            Log.e("My code - Client", "SocketTimeoutException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (SocketException e){
            handleFailure(id);
            Log.e("My code - Client", "SocketException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (EOFException e) {
            handleFailure(id);
            Log.e("My code - Client", "EOFException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (IOException e){
            Log.e("My code - Client", "IOException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (Exception e){
            Log.e("My code - Client", "Exception on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //Log.e("My Code - Client", "End of queryFrom at : "+currentPort);
        return matrixCursor;
    }

    private synchronized Cursor queryFromAll(String id, String selection) {
        BufferedWriter bw=null;
        Socket clientSocket=new Socket();
        Cursor matrixCursor=null;

        Log.e("My Code - Client", "Start of queryFromAll at : "+currentPort+" with destination port : "+id);
        try {
            clientSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id)));
            clientSocket.setSoTimeout(1500);

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(queryFromAllOperation));
            bw.newLine();
            bw.flush();

            bw.write("@");
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            int count = Integer.parseInt(br.readLine());
            Log.e("My Code - Client", "queryFromAll Received Count : "+count);

            String[] columns = new String[] { "key", "value"};
            matrixCursor= new MatrixCursor(columns);
            MatrixCursor tempMatrixCursor = new MatrixCursor(columns);

            for(int k=0;k<count;k++){
                String receivedKey=br.readLine();
                String receivedValue=br.readLine();
                tempMatrixCursor.addRow(new String[] { receivedKey, receivedValue });
            }

            matrixCursor = tempMatrixCursor;

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My Code - Client", "ClientTask UnknownHostException");
        } catch (NumberFormatException e){
            handleFailure(id);
            Log.e("My code - Client", "NumberFormatException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (NullPointerException e) {
            handleFailure(id);
            Log.e("My code - Client", "SocketTimeoutException on port " + currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (SocketTimeoutException e){
            handleFailure(id);
            Log.e("My code - Client", "SocketTimeoutException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (SocketException e){
            handleFailure(id);
            Log.e("My code - Client", "SocketException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        } catch (EOFException e) {
            handleFailure(id);
            Log.e("My code - Client", "EOFException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (IOException e){
            Log.e("My code - Client", "IOException on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }catch (Exception e){
            Log.e("My code - Client", "Exception on port " +currentPort + " due to : " + e);
            e.printStackTrace();
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //Log.e("My Code - Client", "End of queryFromAll at : "+currentPort+" with destination port : "+id);
        return matrixCursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader bReader=null;
            Socket srvSocket = null;

            int messageCounter = 0;
            while (true) {
                try {
                    //Log.e("My Code - Server", "Try block in Server thread");
                    srvSocket = serverSocket.accept();
                    //Log.e("My Code - Server", "Accepted the socket");

                    InputStream iStream = srvSocket.getInputStream();
                    bReader = new BufferedReader(new InputStreamReader(iStream));

                    int operation = Integer.parseInt(bReader.readLine());
                    Log.e("My Code - Server", "The operation to perform is : "+operation);

                    if(operation==nodeRevive){
                        Log.e("My Code - Server", "Start of node revive in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String joinPort = bReader.readLine();

                        if(isFailureDetected==1 && failedNode.equals(joinPort)){
                            failedNode="none";
                            isPortActive.put(joinPort, 1);
                            isFailureDetected=0;
                            //establishDHT();
                            Log.e("My Code - Server", "Added " + joinPort + " to active ports at Revive");
                        }

                        int bufferSize =messageBuffer.getBufferSize();
                        Log.e("My Code - Server", "Message Buffer size : "+bufferSize);

                        if(messageBuffer.inactivePort.equals(joinPort) && bufferSize>0){
                            bWriter.write(String.valueOf(bufferSize));
                            bWriter.newLine();
                            bWriter.flush();

                            Map<String,String> msgBufferMap = messageBuffer.messageBufferMap;
                            for(String k:msgBufferMap.keySet()){
                                bWriter.write(k);
                                bWriter.newLine();
                                bWriter.flush();

                                bWriter.write(msgBufferMap.get(k));
                                bWriter.newLine();
                                bWriter.flush();
                            }
                            Log.e("My Code - Server", "Sent the contents of the buffer");
                            messageBuffer.clearBuffer();
                            //messageBuffer.inactivePort="none";
                        }else{
                            bWriter.write(String.valueOf(0));
                            bWriter.newLine();
                            bWriter.flush();
                        }

                        bReader.readLine();

                        Log.e("My Code - Server", "End of node join in server");

                    }

               /*     if(operation==notify){
                        //Log.e("My Code - Server", "Start of notify in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        successor=bReader.readLine();
                        Log.e("My Code - Server", "Updated successor of "+currentPort+" is : "+successor);

                        predecessor=bReader.readLine();
                        Log.e("My Code - Server", "Updated predecessor of "+currentPort+" is : "+predecessor);

                        maxHash=bReader.readLine();
                        minHash=bReader.readLine();

                        bWriter.write("end");
                        bWriter.newLine();
                        bWriter.flush();

                        //Log.e("My Code - Server", "End of notify in server");

                    }

                    if(operation==lookUpOperation){
                        String receivedKeyHash = bReader.readLine();

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        int prevEmulator = Integer.parseInt(predecessor)/2;
                        if(receivedKeyHash.compareTo(genHash(String.valueOf(prevEmulator)))>0 && receivedKeyHash.compareTo(myPortHash)<=0){
                            bWriter.write(currentPort);
                            bWriter.newLine();
                            bWriter.flush();
                        }
                        else {
                            bWriter.write(lookup(successor, receivedKeyHash));
                            bWriter.newLine();
                            bWriter.flush();
                        }
                        bReader.readLine();

                    }*/

                    if(operation==insertOperation){

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();
                        String receivedValue = bReader.readLine();

                        //Log.e("My Code - Server", "Insert - Received Key : " + receivedKey);
                        //Log.e("My Code - Server", "Insert - Received Value : " + receivedValue);
                        ContentValues cv= new ContentValues();
                        cv.put(KEY,receivedKey);
                        cv.put(VALUE, receivedValue);

                        //Log.e("My Code - Server", "Insert - basic insert");
                        Uri uriToReturn = basicInsert(uri,cv);

                        //Log.e("My Code - Server", "Insert - writing return value");
                        bWriter.write(uriToReturn.toString());
                        bWriter.newLine();
                        bWriter.flush();

                        //Log.e("My Code - Server", "Insert - reading end");
                        bReader.readLine();
                    }

                    else if(operation == queryOperation){
                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();
                        Cursor cursor = basicQuery(uri, null, receivedKey,null,null);

                        bWriter.write(cursor.getString(0));
                        bWriter.newLine();
                        bWriter.flush();

                        bWriter.write(cursor.getString(1));
                        bWriter.newLine();
                        bWriter.flush();

                        bReader.readLine();

                    }

                    else if(operation==deleteOperation){
                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();
                        int rows = basicDelete(uri, receivedKey, null);

                        bWriter.write(String.valueOf(rows));
                        bWriter.newLine();
                        bWriter.flush();
                        ;
                        Log.e("My Code - Server", "deleteOperation sent delCount : "+rows+ " from : "+currentPort);

                        bReader.readLine();
                    }
                    else if(operation == queryFromAllOperation){
                        //Log.e("My Code - Server", "Start of queryFromAll in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();

                        Cursor cursor = basicQuery(uri, null, receivedKey, null,null);

                        bWriter.write(String.valueOf(cursor.getCount()));
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My Code - Server", "queryFromAll sent Count : " + cursor.getCount());

                        for(int l=0;l<cursor.getCount();l++){
                            bWriter.write(cursor.getString(0));
                            bWriter.newLine();
                            bWriter.flush();

                            bWriter.write(cursor.getString(1));
                            bWriter.newLine();
                            bWriter.flush();

                            cursor.moveToNext();

                        }

                          bReader.readLine();
                        //Log.e("My Code - Server", "End of queryFromAll in server.");
                    }

                /*    else if(operation == deleteFromAllOperation){
                        Log.e("My Code - Client", "Start of deleteFromAll in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();

                        int rows = delete(uri, receivedKey, null);

                        bWriter.write(String.valueOf(rows));
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My Code - Client", "deleteFromAll sent delCount : "+rows);

                        bWriter.write(successor);
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My Code - Client", "deleteFromAll sent Successor : " + successor);


                        bReader.readLine();
                        Log.e("My Code - Client", "End of queryFromAll in server. Successor : " + successor);
                    }
*/
                } catch (IOException e) {
                    Log.e("My Code - Server", "Failed to open Server Port due to : " + e);
                    e.printStackTrace();
                    break;
                } /*catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }*/ finally {
                    try {
                        srvSocket.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return null;
        }

        public synchronized Uri basicInsert(Uri uri, ContentValues values)  {

            // TODO Auto-generated method stub
            //Log.e("My Code - Client", " Getting a writable instance of a db");
            SQLiteDatabase database = dbHelper.getWritableDatabase();

            //Log.e("My Code - Client", " Getting a writable instance of a db");

            long rowId=0;
            Uri retUri=null;
            try {
                rowId = database.replace(DBNAME, "", values);
                //Log.e("My Code - Server", " Basic Insert - Replacing element in db");
            }catch (Exception e) {
                //Log.e("My Code - Server", "SQL write failed due to " + e);
                e.printStackTrace();
            }

            if (rowId > 0) {
                //Log.e("My Code - Server", "Basic insertion successful. Row "+rowId+" added to the table");
                getContext().getContentResolver().notifyChange(uri, null);
            }
            Log.v("insert", values.toString());
            return uri;
        }

        public synchronized Cursor basicQuery(Uri uri, String[] projection, String selection, String[] selectionArgs,
                                         String sortOrder) {
            SQLiteDatabase database = dbHelper.getReadableDatabase();

            Cursor c1=null;

            try{

                String hashElement = genHash(selection);
                String[] args = new String[1];
                args[0] = selection;

                if(selection.equals("@")){
                    Log.e("My Code - Server", "Basic Query - Querying for @ ");
                    c1 = database.query(
                            DBNAME,  // The table to query
                            null,                               // The columns to return
                            null,                                    // The columns for the WHERE clause
                            null,                                       // The values for the WHERE clause
                            null,                                     // don't group the rows
                            null,                                     // don't filter by row groups
                            null                                 // The sort order
                    );
                }
                else{
                    Log.e("My Code - Server", "Basic Query - Querying for "+selection);
                        c1 = database.query(
                                DBNAME,  // The table to query
                                projection,                               // The columns to return
                                "key =?",                                    // The columns for the WHERE clause
                                args,                                       // The values for the WHERE clause
                                null,                                     // don't group the rows
                                null,                                     // don't filter by row groups
                                null                                 // The sort order
                        );
                }
                if (c1.moveToFirst())
                    Log.e("My Code - Server", "Basic Query - The column contents for key : " + c1.getString(0) + " value : " + c1.getString(1));
            } catch (Exception e) {
                Log.e("My Code - Server", "SQL query failed due to " + e);
                e.printStackTrace();
            }

            Log.v("query", selection);
            return c1;
        }

        public synchronized int basicDelete(Uri uri, String selection, String[] selectionArgs) {

            SQLiteDatabase database = dbHelper.getWritableDatabase();
            int rowsDeleted=0;
            String[] args = new String[1];
            args[0] = selection;
            try {
                if(selection.equals("@")){
                    Log.e("My Code - Server", "Deleting for @");
                    rowsDeleted = database.delete(DBNAME, null, args);

                }
                else{
                    rowsDeleted = database.delete(DBNAME, "key =?", args);
                }
            }catch (Exception e){
                Log.e("My Code - Server", "SQL delete failed due to " + e);
                e.printStackTrace();
            }

            Log.e("My Code - Server", "Number of rows deleted in basicDelete : " + rowsDeleted);
            return rowsDeleted;
        }


    }


    private String[] findAVD(String hashKey) {
        //Log.e("My Code - Client", "Inside findAVD at : "+currentPort);
        String[] returnArr = new String[3];
        if(hashKey.compareTo(minHash)<0 || hashKey.compareTo(maxHash)>0){
            returnArr[0]=hashToPort.get(dhtList.get(0));
            returnArr[1]=hashToPort.get(dhtList.get(1));
            returnArr[2]=hashToPort.get(dhtList.get(2));
        }
        else {
            for(int i=0;i<dhtList.size()-1;i++){
                if(hashKey.compareTo(dhtList.get(i))>0 && hashKey.compareTo(dhtList.get(i+1))<0){
                    //Log.e("My Code - Client", "The correct partitions are : "+(i+1)%5+", "+(i+2)%5+", "+(i+3)%5);
                    returnArr[0]=hashToPort.get(dhtList.get((i+1)%5));
                    returnArr[1]=hashToPort.get(dhtList.get((i+2)%5));
                    returnArr[2]=hashToPort.get(dhtList.get((i+3)%5));
                }
            }
        }
        Log.e("My Code - Client", "findAVD returns : " + returnArr[0] + ", " + returnArr[1] + ", " + returnArr[2]);

        return returnArr;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
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

    public class MessageBuffer {

        String inactivePort;
        Map<String,String> messageBufferMap = new LinkedHashMap<String, String>();

        public int getBufferSize(){
            return messageBufferMap.size();
        }

        public void clearBuffer() {
            this.messageBufferMap.clear();
            this.inactivePort="none";
            Log.e("My Code", "Cleared the contents of the message buffer");

        }
    }

	private class DBHelper extends SQLiteOpenHelper {

		private static final String SQL_TABLE = "CREATE TABLE " +
				"SimpleDynamoDB" +                       // Table's name
				"(" +                           // The columns in the table
				" key TEXT PRIMARY KEY, " +
				" value TEXT )";

		private static final String SQL_DELETE_ENTRIES =
				"DROP TABLE IF EXISTS SimpleDynamoDB";

		public DBHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
			super(context, name, factory, version);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(SQL_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL(SQL_DELETE_ENTRIES);
			onCreate(db);
		}
	}
}
