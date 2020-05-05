
/*FibinacciHeapNode class to implement a node of fibonacciHeap which contains the node information
        in order to maintain and create the fibonacci heap */
public class FibonacciHeapNode {

    // key which keeps the counter of the hashtag and on the basis of this key we will build the fibonacci heap
    double key ;
    //stores the hashtag
    String data ;
    // reference to the parent node of the current fibonacci heap node
    FibonacciHeapNode parent;
    // refers to the first child node of the current fibonacci heap node
    FibonacciHeapNode firstChild;
    // refers to the left sibling in fibonacci heap
    FibonacciHeapNode leftNode ;
    //refers to the right sibling in fibonacci heap
    FibonacciHeapNode rightNode;
    //stores the child cut value
    boolean childCut;
    //stores the degree of the fibonacciNode(number of child a fibonacciHeap Node have)
    int degree;

//Default constructor. Initializes the right and left pointers along with childcut and degree value
// , making this a circular doubly-linked list.
    public FibonacciHeapNode(String data, double key)
    {
        this.rightNode = this;
        this.leftNode = this;
        this.data = data;
        this.key = key;
        this.childCut=false;
        this.degree=0;
        this.parent=null;
        this.firstChild=null;
    }


    //obtains teh key of the node -- in our case it is the counter of the hashtag
    public  double getKey()
    {
        return key;
    }

    //to get the hashtag from the fibonacciHeap Node
    public  String getData()
    {
        return data;
    }


}
