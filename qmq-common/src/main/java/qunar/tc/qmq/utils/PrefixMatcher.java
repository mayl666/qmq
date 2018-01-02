/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.utils;

import com.googlecode.concurrenttrees.common.CharSequenceUtil;
import com.googlecode.concurrenttrees.common.KeyValuePair;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.NodeFactory;
import com.googlecode.concurrenttrees.radixinverted.ConcurrentInvertedRadixTree;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-27
 */
public class PrefixMatcher<O> {

    static class PrefixMatcherImpl<O> extends ConcurrentRadixTree<O> {

        public PrefixMatcherImpl(NodeFactory nodeFactory) {
            super(nodeFactory);
        }

        public PrefixMatcherImpl(NodeFactory nodeFactory, boolean restrictConcurrency) {
            super(nodeFactory, restrictConcurrency);
        }

        /**
         * Traverses the tree based on characters in the given input, and for
         * each node traversed which encodes a key in the tree, invokes the
         * given {@link KeyValueHandler} supplying it the key which matched that
         * node and the value from the node.
         *
         * @param input           A sequence of characters which controls traversal of the
         *                        tree
         * @param keyValueHandler An object which will be notified of every key and value
         *                        encountered in the input
         */
        protected void scanForKeysAtStartOfInput(CharSequence input, KeyValueHandler keyValueHandler) {
            Node currentNode = super.root;
            int charsMatched = 0;

            final int documentLength = input.length();
            outer_loop:
            while (charsMatched < documentLength) {
                Node nextNode = currentNode.getOutgoingEdge(input.charAt(charsMatched));
                if (nextNode == null) {
                    // Next node is a dead end...
                    // noinspection UnnecessaryLabelOnBreakStatement
                    break outer_loop;
                }

                currentNode = nextNode;
                CharSequence currentNodeEdgeCharacters = currentNode.getIncomingEdge();
                int charsMatchedThisEdge = 0;
                for (int i = 0, j = Math.min(currentNodeEdgeCharacters.length(), documentLength - charsMatched); i < j; i++) {
                    if (currentNodeEdgeCharacters.charAt(i) != input.charAt(charsMatched + i)) {
                        // Found a difference in chars between character in key
                        // and a character in current node.
                        // Current node is the deepest match (inexact match)....
                        break outer_loop;
                    }
                    charsMatchedThisEdge++;
                }
                if (charsMatchedThisEdge == currentNodeEdgeCharacters.length()) {
                    // All characters in the current edge matched, add this
                    // number to total chars matched...
                    charsMatched += charsMatchedThisEdge;
                }
                if (currentNode.getValue() != null) {
                    CharSequence key = input.subSequence(0, charsMatched);
                    O value = getValueForExactKey(key);
                    if (value != null) {
                        keyValueHandler.handle(key, currentNode.getValue());
                    }
                }
            }
        }

        protected void getKey(CharSequence input, KeyValueHandler keyValueHandler) {
            Node currentNode = super.root;
            int charsMatched = 0;
            final int documentLength = input.length();

            outer_loop:
            while (true) {
                Node nextNode = currentNode.getOutgoingEdge(input.charAt(charsMatched));
                if (nextNode == null) {
                    // Next node is a dead end...
                    // noinspection UnnecessaryLabelOnBreakStatement
                    break outer_loop;
                }

                currentNode = nextNode;
                CharSequence currentNodeEdgeCharacters = currentNode.getIncomingEdge();
                if (charsMatched < documentLength) {
                    int charsMatchedThisEdge = 0;
                    for (int i = 0, j = Math.min(currentNodeEdgeCharacters.length(), documentLength - charsMatched); i < j; i++) {
                        if (currentNodeEdgeCharacters.charAt(i) != input.charAt(charsMatched + i)) {
                            // Found a difference in chars between character in key
                            // and a character in current node.
                            // Current node is the deepest match (inexact match)....
                            break outer_loop;
                        }
                        charsMatchedThisEdge++;
                    }
                    charsMatched += charsMatchedThisEdge;
                    if (charsMatched < documentLength) {
                        continue;
                    }
                }

                if (charsMatched == documentLength) {
                    Object value = currentNode.getValue();
                    if (value != null) {
                        keyValueHandler.handle(input, value);
                    }
                }

                if (charsMatched >= documentLength) {
                    break;
                }
            }
        }

        protected void scanForInputAtStartOfInput(CharSequence input, KeyValueHandler keyValueHandler) {
            if (input == null || input.length() == 0 || keyValueHandler == null) {
                return;
            }
            Node currentNode = super.root;
            int charsMatched = 0;
            StringBuilder prefix = new StringBuilder(input.length());
            final int documentLength = input.length();

            outer_loop:
            while (true) {
                Node nextNode = currentNode.getOutgoingEdge(input.charAt(charsMatched));
                if (nextNode == null) {
                    // Next node is a dead end...
                    // noinspection UnnecessaryLabelOnBreakStatement
                    break outer_loop;
                }

                currentNode = nextNode;
                CharSequence currentNodeEdgeCharacters = currentNode.getIncomingEdge();
                if (charsMatched < documentLength) {
                    int charsMatchedThisEdge = 0;
                    for (int i = 0, j = Math.min(currentNodeEdgeCharacters.length(), documentLength - charsMatched); i < j; i++) {
                        if (currentNodeEdgeCharacters.charAt(i) != input.charAt(charsMatched + i)) {
                            // Found a difference in chars between character in key
                            // and a character in current node.
                            // Current node is the deepest match (inexact match)....
                            break outer_loop;
                        }
                        charsMatchedThisEdge++;
                    }
                    charsMatched += charsMatchedThisEdge;
                    prefix.append(currentNodeEdgeCharacters);
                    if (charsMatched < documentLength) {
                        continue;
                    }
                }

                if (charsMatched == documentLength) {
                    Object value = currentNode.getValue();
                    if (value != null) {
                        keyValueHandler.handle(input, value);
                    }
                }

                List<Node> outgoingEdges = currentNode.getOutgoingEdges();
                if (outgoingEdges == null || outgoingEdges.size() == 0) {
                    break;
                }
                for (Node node : outgoingEdges) {
                    StringBuilder startPoint = new StringBuilder(prefix);
                    CharSequence keyEdge = node.getIncomingEdge();
                    Object value = node.getValue();
                    if (value != null) {
                        keyValueHandler.handle(startPoint.append(keyEdge), value);
                    }
                }
                break;
            }
        }

        interface KeyValueHandler {
            void handle(CharSequence key, Object value);
        }
    }

    private final PrefixMatcherImpl<O> radixTree;

    /**
     * Creates a new {@link ConcurrentInvertedRadixTree} which will use the
     * given {@link NodeFactory} to create nodes.
     *
     * @param nodeFactory An object which creates {@link Node} objects on-demand, and
     *                    which might return node implementations optimized for storing
     *                    the values supplied to it for the creation of each node
     */
    public PrefixMatcher(NodeFactory nodeFactory) {
        this.radixTree = new PrefixMatcherImpl<O>(nodeFactory);
    }

    /**
     * Creates a new {@link ConcurrentInvertedRadixTree} which will use the
     * given {@link NodeFactory} to create nodes.
     *
     * @param nodeFactory         An object which creates {@link Node} objects on-demand, and
     *                            which might return node implementations optimized for storing
     *                            the values supplied to it for the creation of each node
     * @param restrictConcurrency If true, configures use of a
     *                            {@link java.util.concurrent.locks.ReadWriteLock} allowing
     *                            concurrent reads, except when writes are being performed by
     *                            other threads, in which case writes block all reads; if false,
     *                            configures lock-free reads; allows concurrent non-blocking
     *                            reads, even if writes are being performed by other threads
     */
    public PrefixMatcher(NodeFactory nodeFactory, boolean restrictConcurrency) {
        this.radixTree = new PrefixMatcherImpl<O>(nodeFactory, restrictConcurrency);
    }

    public O put(CharSequence key, O value) {
        return radixTree.put(key, value);
    }

    public O putIfAbsent(CharSequence key, O value) {
        return radixTree.putIfAbsent(key, value);
    }

    public boolean remove(CharSequence key) {
        return radixTree.remove(key);
    }

    public O getValueForExactKey(CharSequence key) {
        return radixTree.getValueForExactKey(key);
    }


    public Set<CharSequence> getKeysPrefixIn(CharSequence document) {
        final Set<CharSequence> results = new LinkedHashSet<CharSequence>();

        radixTree.scanForKeysAtStartOfInput(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                String keyString = CharSequenceUtil.toString(key);
                results.add(keyString);
            }
        });

        return results;
    }


    public Set<O> getValuesForKeysPrefixIn(CharSequence document) {
        final Set<O> results = new LinkedHashSet<O>();
        radixTree.scanForKeysAtStartOfInput(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                @SuppressWarnings({"unchecked"})
                O valueTyped = (O) value;
                results.add(valueTyped);
            }
        });
        return results;
    }

    public Set<KeyValuePair<O>> getKeyValuePairsForKeysPrefixIn(CharSequence document) {
        final Set<KeyValuePair<O>> results = new LinkedHashSet<KeyValuePair<O>>();
        radixTree.scanForKeysAtStartOfInput(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                @SuppressWarnings({"unchecked"})
                O valueTyped = (O) value;
                String keyString = CharSequenceUtil.toString(key);
                results.add(new ConcurrentRadixTree.KeyValuePairImpl<O>(keyString, valueTyped));
            }
        });
        return results;
    }

    public Set<CharSequence> getKeysForInputPrefixIn(final CharSequence document) {
        final Set<CharSequence> results = new LinkedHashSet<CharSequence>();
        radixTree.scanForInputAtStartOfInput(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                String keyString = CharSequenceUtil.toString(key);
                results.add(keyString);
            }
        });
        return results;
    }

    public Set<O> getValuesForInputPrefixIn(CharSequence document) {
        final Set<O> results = new LinkedHashSet<O>();
        radixTree.scanForInputAtStartOfInput(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                @SuppressWarnings({"unchecked"})
                O valueTyped = (O) value;
                results.add(valueTyped);
            }
        });
        return results;
    }

    public Set<KeyValuePair<O>> getKeyValuePairsForInputPrefixIn(CharSequence document) {
        final Set<KeyValuePair<O>> results = new LinkedHashSet<KeyValuePair<O>>();
        radixTree.scanForInputAtStartOfInput(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                @SuppressWarnings({"unchecked"})
                O valueTyped = (O) value;
                String keyString = CharSequenceUtil.toString(key);
                results.add(new ConcurrentRadixTree.KeyValuePairImpl<O>(keyString, valueTyped));
            }
        });
        return results;
    }

    public KeyValuePair<O> get(CharSequence document) {
        final List<KeyValuePair<O>> results = new ArrayList<>(1);
        radixTree.getKey(document, new PrefixMatcherImpl.KeyValueHandler() {
            @Override
            public void handle(CharSequence key, Object value) {
                @SuppressWarnings({"unchecked"})
                O valueTyped = (O) value;
                String keyString = CharSequenceUtil.toString(key);
                results.add(new ConcurrentRadixTree.KeyValuePairImpl<O>(keyString, valueTyped));
            }
        });
        return results.size() == 0 ? null : results.get(0);
    }

    public Node getNode() {
        return radixTree.getNode();
    }
}
