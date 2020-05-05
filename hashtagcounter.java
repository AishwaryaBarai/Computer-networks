import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class hashtagcounter  {

    public static void main(String[] args) throws IOException {
        Hashtable<String,FibonacciHeapNode> hashTable=new Hashtable<String,FibonacciHeapNode>();
        FibonacciHeap fh = new FibonacciHeap();
        BufferedReader fin=new BufferedReader(new FileReader(args[0]));
        BufferedWriter fout=null;
        if(args.length==2){
             fout=new BufferedWriter(new FileWriter(args[1]));
        }
        while(true){
            String readLine = fin.readLine();
            if(readLine.startsWith("#")){
                String[] split = readLine.split(" ");
                if(hashTable.get(split[0])!=null){
                    fh.increaseKey(hashTable.get(split[0]),Double.parseDouble(split[1]));
                }else{
                    FibonacciHeapNode node=new FibonacciHeapNode(split[0],Double.parseDouble(split[1]));
                    fh.insert(node);
                    hashTable.put(split[0],node);
                }
            }else if(readLine.equalsIgnoreCase("stop")){
                break;
            }else{
                try{
                    Integer number=Integer.parseInt(readLine);
                    if(number>20){
                        number=20;
                    }
                    List<FibonacciHeapNode> tempList=new ArrayList<>();
                    while(number>0){
                        FibonacciHeapNode fibonacciHeapNode = fh.max();
                        //System.out.println(fibonacciHeapNode.getData());
                        tempList.add(new FibonacciHeapNode(fibonacciHeapNode.getData(),fibonacciHeapNode.getKey()));
                        if(fout!=null){
                            fout.write(fibonacciHeapNode.getData().substring(1));
                            if(number>1){
                                fout.write(",");
                            }else{
                                fout.write("\n");
                            }
                            fout.flush();
                        }else{
                            System.out.print(fibonacciHeapNode.getData().substring(1));
                            if(number>1){
                                System.out.print(",");
                            }else{
                                System.out.println();
                            }
                        }
                        fh.extractMax();
                        hashTable.remove(fh.max().getData());
                        number--;
                    }
                    for(FibonacciHeapNode  node: tempList){
                        hashTable.put(node.getData(),node);
                        fh.insert(node);
                    }
                }catch(NumberFormatException e){
                    System.out.println("Input file corrupt ");
                    break;
                }
            }
        }


    }






    //



}
