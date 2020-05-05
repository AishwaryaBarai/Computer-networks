
//FibonacciHeap class to implement the fibonacciHeap datastructure to store the hashtag and its occurance counter

import java.util.ArrayList;
import java.util.List;

public class FibonacciHeap {


    //points to the maximum node in Fibonacci Heap
    private FibonacciHeapNode maxNode;

   //keeps track of the number of nodes in fibonacci heap
    private int numNodes;

    //default constructor
    public FibonacciHeap()
    {
    }


    /**
     * Inserts a new data element into the heap.Directly the node is added into the root list
     * @param node new node to insert into heap
     */
    public void insert(FibonacciHeapNode node)
    {
        //node.key = key;

        if (maxNode != null) {
            node.leftNode = maxNode;
            node.rightNode = maxNode.rightNode;
            maxNode.rightNode.leftNode = node;
            maxNode.rightNode = node;

            if (node.key > maxNode.key) {
                maxNode = node;
            }
        } else {
            maxNode = node;
        }

        numNodes+=1;
    }


    /**
     * Returns the largest element in the heap.i.e It returns the node having hastag with maximum occurance
     *
     * @return heap node with the largest key
     */
    public FibonacciHeapNode max()
    {
        return maxNode;
    }


    /**
     * Removes the largest element from the heap. This will cause the trees in
     * the heap to be consolidated, if necessary.
     * @return node with the largest key
     */
    public FibonacciHeapNode extractMax()
    {
        FibonacciHeapNode z = maxNode;

        if (z != null) {
            int numberOfChildren = z.degree;
            FibonacciHeapNode x = z.firstChild;
            for (int i = 0; i < numberOfChildren; i++) {
                FibonacciHeapNode rightChildTemp = x.rightNode;
                //remove the child from the child list of the maxNode
                x.leftNode.rightNode = x.rightNode;
                x.rightNode.leftNode = x.leftNode;
                //add the child node to the root list
                x.leftNode = maxNode;
                x.rightNode = maxNode.rightNode;
                maxNode.rightNode.leftNode = x;
                maxNode.rightNode = x;
                //set the parent of child as null as it is not part of root nodes
                x.parent = null;
                x = rightChildTemp;
            }
            //remove the maxNode from the rootlist now
            z.leftNode.rightNode = z.rightNode;
            z.rightNode.leftNode = z.leftNode;
            //check if there was only one node in the fibonacci Heap
            if (z.rightNode == z) {
                maxNode = null;
            } else {
                maxNode = z.rightNode;
                //consolidate by linking roots of equal degree until only one node of each degree left
                consolidate();
            }
            //decrement the size of heap
            numNodes--;
        }
        return z;
    }



    //consolidate function to link the roots of equal degrees until only one node of each degree left
    protected void consolidate()
    {
        int arraySize =((int) Math.floor(Math.log(numNodes) * (1.0 / Math.log((1.0 + Math.sqrt(5.0)) / 2.0)))) + 1;
                //((int) Math.floor(Math.log(numNodes) * (1.0 / Math.log((1.0 + Math.sqrt(5.0)) / 2.0)))) + 1;

        List<FibonacciHeapNode> array =
                new ArrayList<FibonacciHeapNode>(arraySize);

        // Initialize degree array
        for (int i = 0; i < arraySize; i++) {
            array.add(null);
        }

        // Find the number of root nodes.
        int rootNodes = 0;
        FibonacciHeapNode x = maxNode;

        if (x != null) {
            rootNodes++;
            x = x.rightNode;

            while (x != maxNode) {
                rootNodes++;
                x = x.rightNode;
            }
        }

        // For each node in root list do...
        while (rootNodes > 0) {
            // Access this node's degree..
            int degree = x.degree;
            FibonacciHeapNode next = x.rightNode;

            // check if there's another of the same degree.
            while(true) {
                //System.out.println(degree);
                //System.out.println(array.size());
                //System.out.println("number of nodes"+array.size());

                //check if there is any existing node with the given degree
                FibonacciHeapNode y = array.get(degree);
                if (y == null) {
                    // if not break
                    break;
                }
                //else consolidate the node x with the existing node in the array
                //by making the one the child of another on the basis of key
                if (x.key < y.key) {
                    FibonacciHeapNode temp = y;
                    y = x;
                    x = temp;
                }

                link(y, x);

                //degree is incremented so remove the node from the array
                array.set(degree, null);
                degree++;
            }

            // set the node with the increased degree for later use
            array.set(degree, x);

            x = next;
            //decrease the number of root nodes
            rootNodes--;
        }

        // Set max to null and
        maxNode = null;


        //find the maxNode and create a new root node list
        for (int i = 0; i < arraySize; i++) {
            FibonacciHeapNode y = array.get(i);
            if (y == null) {
                continue;
            }

            if (maxNode != null) {
                // First remove node from root list as somenodes might be still in root list
                y.leftNode.rightNode = y.rightNode;
                y.rightNode.leftNode = y.leftNode;

                // Now add to root list, again.
                y.leftNode = maxNode;
                y.rightNode = maxNode.rightNode;
                maxNode.rightNode = y;
                y.rightNode.leftNode = y;

                if (y.key > maxNode.key) {
                    maxNode = y;
                }
            } else {
                maxNode = y;
            }
        }
    }


    /**
     Link function to link the y fibonacciHeapNode as child of x FibonacciHeapNode
     */
    protected void link(FibonacciHeapNode y, FibonacciHeapNode x)
    {
        // remove y from root list of heap
        y.leftNode.rightNode = y.rightNode;
        y.rightNode.leftNode = y.leftNode;

        // make y a child of x
        y.parent = x;

        if (x.firstChild == null) {
            x.firstChild = y;
            y.rightNode = y;
            y.leftNode = y;
        } else {
            y.leftNode = x.firstChild;
            y.rightNode = x.firstChild.rightNode;
            x.firstChild.rightNode = y;
            y.rightNode.leftNode = y;
        }

        // degree of x
        x.degree++;

        //set the child cut as false
        y.childCut = false;
    }


    /**
     * Increases the key value for a heap node, given the new value to take on.
     * Consolidation is not required in increase key, Adjustments are made based on the child cut value .
     * @param x : node whose key has to be increased
     * @param k : quantity by which key has to be increased
     */
    public void increaseKey(FibonacciHeapNode x, double k)
    {
        x.key += k;

        FibonacciHeapNode y = x.parent;

        //in case the increase key value is more than the parent key value
        if ((y != null) && (x.key > y.key)) {
            cut(x, y);
            cascadingCut(y);
        }

        // if the increased key becomes more than the maxNode
        if (x.key > maxNode.key) {
            maxNode = x;
        }
    }


    /**
     * The reverse of the link operation: removes x from the child list of y.
     * @param x child of y to be removed from y's child list
     * @param y parent of x about to lose a child
     */
    protected void cut(FibonacciHeapNode x, FibonacciHeapNode y)
    {
        // remove x from childlist of y and decrement degree of y
        x.leftNode.rightNode = x.rightNode;
        x.rightNode.leftNode = x.leftNode;
        y.degree--;

        //in case the child value of y was set to x
        if (y.firstChild == x) {
            y.firstChild = x.rightNode;
        }

        //if x was the only child of y
        if (y.degree == 0) {
            y.firstChild = null;
        }

        // add x to root list of heap
        x.leftNode = maxNode;
        x.rightNode = maxNode.rightNode;
        maxNode.rightNode = x;
        x.rightNode.leftNode = x;

        // set parent[x] to nil
        x.parent = null;

        // set mark[x] to false
        x.childCut = false;
    }


    /**
     * Performs a cascading cut operation. This cuts y from its parent and then
     * does the same for its parent, and so on up the tree.
     * @param y node to perform cascading cut on
     */
    protected void cascadingCut(FibonacciHeapNode y)
    {
        FibonacciHeapNode z = y.parent;

        // if there's a parent...
        if (z != null) {
            // if y's childcut value if false , set it to true
            if (!y.childCut) {
                y.childCut = true;
            } else {
                // if the child cut is already true , then perform the same operation on y as we did on x
                cut(y, z);
                // cut its parent as well
                cascadingCut(z);
            }
        }
    }



}
