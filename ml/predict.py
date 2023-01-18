import torch
from torchvision import models
from PIL import Image
from torchvision import transforms
import torch.nn.functional as F
import torchvision.utils as utils
import logging

def predict(filepath, model):
        if model == "ResNet":
                return predictResNet(filepath)
        elif model == "AlexNet":
                return predictAlexNet(filepath)
        else:
                logging.debug("Model {} not found".format(model))
                return None, None

def predictAlexNet(filepath):
        data_transforms = transforms.Compose([        
            transforms.Resize((224,224)),             # resize the input to 224x224
            transforms.ToTensor(),              # put the input to tensor format
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])  # normalize the input
            # the normalization is based on images from ImageNet
        ])

        # obtain the file path of the testing image
        test_image_filepath = filepath
        #print(test_image_filepath)

        # open the testing image
        img = Image.open(test_image_filepath).convert('RGB')

        # pre-process the input
        transformed_img = data_transforms(img)

        # form a batch with only one image
        batch_img = torch.unsqueeze(transformed_img, 0)

        # load pre-trained AlexNet model
        alexnet = models.alexnet(weights="AlexNet_Weights.DEFAULT")
        # put the model to eval mode for testing
        alexnet.eval()

        # obtain the output of the model
        output = alexnet(batch_img)

        # map the class no. to the corresponding label
        with open('ml/imagenet_classes.txt') as labels:
            classes = [i.strip() for i in labels.readlines()]
        
        # sort the probability vector in descending order
        sorted, indices = torch.sort(output, descending=True)
        percentage = F.softmax(output, dim=1)[0] * 100.0
        # obtain the first 5 classes (with the highest probability) the input belongs to
        results = [(classes[i], percentage[i].item()) for i in indices[0][:5]]
        return results[0][0], results[0][1]

def predictResNet(filepath):
        resnet = models.resnet18(weights='ResNet18_Weights.DEFAULT')
        img_cat = Image.open(filepath).convert('RGB')
        preprocess = transforms.Compose([
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            )])
        #
        # Pass the image for preprocessing and the image preprocessed
        #
        img_cat_preprocessed = preprocess(img_cat)
        #
        # Reshape, crop, and normalize the input tensor for feeding into network for evaluation
        #
        batch_img_cat_tensor = torch.unsqueeze(img_cat_preprocessed, 0)
        #
        # Resnet is required to be put in evaluation mode in order
        # to do prediction / evaluation
        #
        resnet.eval()
        #
        # Get the predictions of image as scores related to how the loaded image
        # matches with 1000 ImageNet classes. The variable, out is a vector of 1000 scores
        #
        out = resnet(batch_img_cat_tensor)

        #
        # Load the file containing the 1,000 labels for the ImageNet dataset classes
        #
        with open('ml/imagenet_classes.txt') as f:
            labels = [line.strip() for line in f.readlines()]
        #
        # Find the index (tensor) corresponding to the maximum score in the out tensor.
        # Torch.max function can be used to find the information
        #
        _, index = torch.max(out, 1)
        #
        # Find the score in terms of percentage by using torch.nn.functional.softmax function
        # which normalizes the output to range [0,1] and multiplying by 100
        #
        percentage = torch.nn.functional.softmax(out, dim=1)[0] * 100
        #
        # Print the name along with score of the object identified by the model
        #
        return (labels[index[0]], percentage[index[0]].item())