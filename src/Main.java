import org.omg.CORBA.portable.Streamable;
import rTree.*;
import spatialIndex.*;
import storageManager.*;

import javax.swing.text.rtf.RTFEditorKit;
import java.io.*;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        PropertySet ps=new PropertySet();
        ps.setProperty("Overwrite",false);
        ps.setProperty("FileName","test1");
        ps.setProperty("PageSize",100);
        IStorageManager sm= SpatialIndex.createDiskStorageManager(ps);
//        IStorageManager sm=SpatialIndex.createMemoryStorageManager(ps);
        RTree rTree=new RTree(ps,sm);
//        double[] f = { 5, 30, 25, 35, 15, 38, 23, 50, 10, 23, 30, 28, 13, 10, 18, 15, 23, 10, 28, 20, 28, 30, 33, 40, 38,
//                13, 43, 30, 35, 37, 40, 43, 45, 8, 50, 50, 23, 55, 28, 70, 10, 65, 15, 70, 10, 58, 20, 63, };
//        String[] s={"香格里拉酒店","7天宾馆","大风吹烧烤","阿里巴巴烧烤","七号小院烧烤","如家宾馆","速8宾馆","老北京火锅",
//                "海月时尚宾馆","海底捞火锅","小龙坎火锅","大汉堡","巴洛克汉堡","春天宾馆","如家精选宾馆","西苑酒店"};
//        int k=0;
//        for(int i=0;i<f.length;){
//            double[] d1={f[i++],f[i++]};
//            Point p1=new Point(d1);
//            double[] d2={f[i++],f[i++]};
//            Point p2=new Point(d2);
//            Region mbr=new Region(p1,p2);
//            rTree.insertData(s[k++].getBytes(),mbr,node++);
//        }
//        double[] dd1={10,20};
//        double[] dd2={30,32};
//        Point pp1=new Point(dd1);
//        Point pp2=new Point(dd2);
//        Region r=new Region(pp1,pp2);
//        rTree.intersectionQuery(r);
//        rTree.containmentQuery(r);
        readFromFile(rTree);
//        readFromBuffer(rTree);
        Node root=rTree.readNode(rTree.rootID);
//        ArrayList<Node> nl=new ArrayList<>();
//        for (int i=0;i<root.getIdentifier();i++)
//            nl.add(rTree.readNode(root.getChildIdentifier(i)));
//        for (Node n : nl){
//            System.out.println(n.toString());
//        }

//        Region[] mmbr=root.getpMBR();
//        System.out.println(mmbr.length);
//        for (int i=0;i<mmbr.length;i++){
//            System.out.println(mmbr[i].toString());
//        }
//        byte[] d={1,2,3,4};
//        System.out.println(mbr.toString());
//        rTree.insertData(d,mbr,0);
//        System.out.println(rTree.toString());
//        System.out.println(root.toString()+"\n");
//        Node rl=rTree.readNode(6);
//        Node rr=rTree.readNode(6);
//        System.out.println(rr.isLeaf());
//        System.out.println(rl.toString()+"\n");
        HashMap<String,Integer> map=new HashMap<>();
        double[] dd1={200,200};
        double[] dd2={800,800};
        Region r=new Region(dd1,dd2);
        long start=System.currentTimeMillis();
        List l = root.countKeyword(map,r);
        long end=System.currentTimeMillis();
        System.out.println(l);
        System.out.println("Run time:"+(end-start)+"ms");
    }
    //100000  有剔除 651(756)ms  无剔除  903(1012)ms
    //1000000 有剔除 6251ms 无剔除  9236ms

    public static void readFromFile(RTree rTree) throws IOException {
        int node=0;
        File file=new File("D:\\codeWorkspace\\RTT\\test106.dat");
        Reader reader=new FileReader(file);
        BufferedReader bf=new BufferedReader(reader);
        String s=null;
        String[] ss1=null,ss2=null;
        while ((s=bf.readLine())!=null){
            ss1=s.split(" ");
            double[] d1={Double.valueOf(ss1[0]),Double.valueOf(ss1[1])};
            Point p1=new Point(d1,ss1[2].getBytes());
            s=bf.readLine();
            ss2=s.split(" ");
            double[] d2={Double.valueOf(ss2[0]),Double.valueOf(ss2[1])};
            Point p2=new Point(d2,ss2[2].getBytes());
            Region mbr=new Region(p1,p2);
            int length=ss1[2].getBytes().length+ss2[2].getBytes().length+1;
            byte[] data=new byte[length];
            System.arraycopy(ss1[2].getBytes(),0,data,0,ss1[2].getBytes().length);
            System.arraycopy(",".getBytes(),0,data,ss1[2].getBytes().length,1);
            System.arraycopy(ss2[2].getBytes(),0,data,ss1[2].getBytes().length+1,ss2[2].getBytes().length);
            rTree.insertData(data,mbr,node++);
        }
    }

    public static void readFromRAFile(RTree rTree) throws IOException {
        int node=1;
        RandomAccessFile file=new RandomAccessFile("D:\\codeWorkspace\\RTT\\test1000000.dat","rw");
        for (int i=0;i<500000;i++){
            double[] d1={file.readDouble(),file.readDouble()};
            String s1=file.readUTF();
            Point p1=new Point(d1,s1.getBytes());
            double[] d2={file.readDouble(),file.readDouble()};
            String s2=file.readUTF();
            Point p2=new Point(d2,s2.getBytes());
            Region mbr=new Region(p1,p2);
            byte[] data=new byte[s1.getBytes().length+s2.getBytes().length+1];
            System.arraycopy(s1.getBytes(),0,data,0,s1.getBytes().length);
            System.arraycopy(",".getBytes(),0,data,s1.getBytes().length,1);
            System.arraycopy(s2.getBytes(),0,data,s1.getBytes().length+1,s2.getBytes().length);
            rTree.insertData(data,mbr,node++);
        }
//        System.out.println("node:"+node);
    }

    public static void readFromBuffer(RTree rTree){
        int node=0;
        for (int i=0;i<7;i++){
            byte[] data=("数据-"+String.valueOf(i)+",结果-"+String.valueOf(i)).getBytes();
            double[] d1={i,i};
            double[] d2={2*i,2*i};
            Region mbr=new Region(d1,d2);
            rTree.insertData(data,mbr,node++);
        }
    }

//    public static List sortMap(HashMap map){
//        Map<String,Integer> mmap=new HashMap<>(map);
//        List<Map.Entry<String,Integer>> list= new ArrayList<Map.Entry<String, Integer>>(mmap.entrySet());
//        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
//            @Override
//            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
//                return o2.getValue().compareTo(o1.getValue());
//            }
//        });
//        return list;
//    }
}
